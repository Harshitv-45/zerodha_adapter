from kiteconnect import KiteTicker
from Zerodha.zerodha_mapper import ZerodhaMapper
import threading


class ZerodhaWebSocket:
    def __init__(
        self,
        api_key: str,
        access_token: str,
        user_id: str,
        redis_client,
        formatter,
        request_data_mapping: dict,
        logger,
    ):
        self.api_key = api_key
        self.access_token = access_token
        self.user_id = user_id

        self.redis_client = redis_client
        self.formatter = formatter
        self.request_data_mapping = request_data_mapping
        self.logger = logger

        self.kws = None
        self.is_connected = False
        self.should_reconnect = True  # Enable auto-reconnect

        # order_id -> last known state
        self.order_state_cache = {}

        # >>> ADDED: cache WS updates that arrive before REST mapping
        self.pending_ws_updates = {}
        
        # >>> ADDED: lock for thread-safe access to pending_ws_updates
        self.pending_lock = threading.Lock()

    # ---------------------------------------------------------
    def start(self):
        self.should_reconnect = True  # Enable reconnection on start
        ws_token = f"{self.access_token}&user_id={self.user_id}"

        self.kws = KiteTicker(self.api_key, ws_token)
        self.kws.on_connect = self._on_connect
        self.kws.on_close = self._on_close
        self.kws.on_error = self._on_error
        self.kws.on_order_update = self._on_order_update

        self.kws.connect(threaded=True)

    def stop(self):
        self.should_reconnect = False  # Prevent auto-reconnect on manual stop
        if self.kws:
            self.logger.info("[WS] Stopping WebSocket connection")
            try:
                self.kws.close()
                self.is_connected = False
                
                # Clear state caches to prevent stale data on reconnect
                self.order_state_cache.clear()
                self.pending_ws_updates.clear()
                self.logger.info("[WS] State cleared")
                
            except Exception as e:
                self.logger.error(f"[WS] Error during stop: {e}")
            finally:
                self.kws = None

    # ---------------------------------------------------------
    def _on_connect(self, ws, response):
        self.is_connected = True
        self.logger.info("[WS] Connected to Zerodha")

    def _on_close(self, ws, code, reason):
        self.is_connected = False
        
        # If manual stop (should_reconnect is False), just return silently
        if not self.should_reconnect:
            return

        self.logger.warning(f"[WS] Closed: {code} {reason}")
        
        # Auto-reconnect if needed
        if self.should_reconnect:
            self.logger.info("[WS] Reconnecting in 3 seconds...")
            import time
            time.sleep(3)
            if self.kws and self.should_reconnect:
                try:
                    self.kws.connect(threaded=True)
                except Exception as e:
                    self.logger.error(f"[WS] Reconnection failed: {e}")

    def _on_error(self, ws, code, reason):
        # Suppress error logging during manual stop (expected behavior)
        if not self.should_reconnect:
            return
        self.logger.error(f"[WS] Error: {code} {reason}")

    # ---------------------------------------------------------
    def _on_order_update(self, ws, data):
        try:
            # =====================================================
            # RAW ZERODHA WS PAYLOAD
            # =====================================================
            self.logger.info(
                "ZERODHA_WS_RESPONSE | payload=%s",
                data
            )

            order_id = data.get("order_id")
            if not order_id:
                return

            order_id = str(order_id)
            status = (data.get("status") or "").upper()

            # >>> CHANGED: Use lock for thread-safe check and cache
            with self.pending_lock:
                blitz_request = self.request_data_mapping.get(order_id)

                # >>> CHANGED: cache WS update instead of dropping it
                if not blitz_request:
                    self.logger.warning(
                        "ZERODHA_WS_PENDING | order_id=%s payload=%s",
                        order_id,
                        data
                    )
                    self.pending_ws_updates[order_id] = data
                    return

            prev = self.order_state_cache.get(order_id)

            price = data.get("price")
            qty = data.get("quantity")
            pending_qty = int(data.get("pending_quantity") or 0)

            # =====================================================
            # IGNORE UPDATE (exchange noise)
            # =====================================================
            if status == "UPDATE":
                self.order_state_cache[order_id] = {
                    **(prev or {}),
                    "last_update_seen": True
                }
                return

            # =====================================================
            # MODIFY CONFIRMATION → ONE Replaced
            # =====================================================
            if (
                status == "OPEN"
                and prev
                and prev.get("status") == "OPEN"
                and (
                    price != prev.get("price")
                    or qty != prev.get("quantity")
                )
            ):
                order_log = ZerodhaMapper.to_blitz_orderlog(
                    zerodha_data=data,
                    blitz_request=blitz_request
                )
                order_log.OrderStatus = "Replaced"

                self.logger.info(
                    "BLITZ_RESPONSE | order_id=%s payload=%s",
                    order_id,
                    order_log.__dict__
                )

                self.redis_client.publish(
                    self.formatter.order_update(order_log)
                )

                self.order_state_cache[order_id] = {
                    "status": "OPEN",
                    "price": price,
                    "quantity": qty,
                }

                self.logger.info(
                    f"[WS] MODIFY published | order_id={order_id}"
                )
                return

            # =====================================================
            # CANCEL → ignore intermediate
            # =====================================================
            if status == "CANCELLED" and pending_qty > 0:
                return

            # =====================================================
            # FINAL CANCEL
            # =====================================================
            if status == "CANCELLED":
                order_log = ZerodhaMapper.to_blitz_orderlog(
                    zerodha_data=data,
                    blitz_request=blitz_request
                )
                order_log.OrderStatus = "Cancelled"
                order_log.LeavesQuantity = 0

                self.logger.info(
                    "BLITZ_RESPONSE | order_id=%s payload=%s",
                    order_id,
                    order_log.__dict__
                )

                self.redis_client.publish(
                    self.formatter.order_update(order_log)
                )

                self.order_state_cache[order_id] = {
                    "status": "CANCELLED"
                }

                self.logger.info(
                    f"[WS] CANCEL published | order_id={order_id}"
                )
                return

            # =====================================================
            # REJECTED (Insufficient funds, margin issues, etc.)
            # =====================================================
            if status == "REJECTED":
                order_log = ZerodhaMapper.to_blitz_orderlog(
                    zerodha_data=data,
                    blitz_request=blitz_request
                )
                order_log.OrderStatus = "Rejected"
                order_log.LeavesQuantity = 0
                order_log.CancelRejectReason = data.get("status_message") or "Order rejected"

                self.logger.info(
                    "BLITZ_RESPONSE | order_id=%s payload=%s",
                    order_id,
                    order_log.__dict__
                )

                self.redis_client.publish(
                    self.formatter.order_update(order_log)
                )

                self.order_state_cache[order_id] = {
                    "status": "REJECTED"
                }

                self.logger.info(
                    f"[WS] REJECTED published | order_id={order_id}"
                )
                return

            # =====================================================
            # COMPLETE (Filled)
            # =====================================================
            if status == "COMPLETE":
                order_log = ZerodhaMapper.to_blitz_orderlog(
                    zerodha_data=data,
                    blitz_request=blitz_request
                )

                self.logger.info(
                    "BLITZ_RESPONSE | order_id=%s payload=%s",
                    order_id,
                    order_log.__dict__
                )

                self.redis_client.publish(
                    self.formatter.order_update(order_log)
                )

                self.order_state_cache[order_id] = {
                    "status": "COMPLETE"
                }
                return

            # =====================================================
            # FIRST OPEN → New Order
            # =====================================================
            if status == "OPEN" and not prev:
                order_log = ZerodhaMapper.to_blitz_orderlog(
                    zerodha_data=data,
                    blitz_request=blitz_request
                )

                self.logger.info(
                    "BLITZ_RESPONSE | order_id=%s payload=%s",
                    order_id,
                    order_log.__dict__
                )

                self.redis_client.publish(
                    self.formatter.order_update(order_log)
                )

                self.order_state_cache[order_id] = {
                    "status": "OPEN",
                    "price": price,
                    "quantity": qty,
                }

                self.logger.info(
                    f"[WS] NEW order published | order_id={order_id}"
                )
                return

        except Exception as e:
            self.logger.error(
                f"[WS] Failed to process order update: {e}",
                exc_info=True
            )
