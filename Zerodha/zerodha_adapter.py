"""
Zerodha (KITE) Adapter – Clean Production Version

Responsibilities:
- Auto-login (TOTP)
- Load instruments CSV
- Start WebSocket (called by TPOMS)
- Handle PLACE / MODIFY / CANCEL orders
- Cache Blitz request data (temp + permanent)
- Proper error handling via mapper
"""

# ============================================================
# Standard imports
# ============================================================
from typing import Any, Dict, Optional

# ============================================================
# Project imports
# ============================================================
import config
from common.redis_client import RedisClient
from common.message_formatter import MessageFormatter
from common.logging_setup import EntityLogger

# Zerodha imports
from Zerodha.api.autologin import get_access_token
from Zerodha.api.instruments import ZerodhaInstruments
from Zerodha.api.orders import ZerodhaOrderAPI
from Zerodha.zerodha_websocket import ZerodhaWebSocket
from Zerodha.zerodha_mapper import ZerodhaMapper


class ZerodhaAdapter:
    """
    Zerodha Adapter controlled by TPOMS lifecycle.
    """

    def __init__(
        self,
        entity_id: str,
        creds: Dict[str, Any],
        logger: Optional[EntityLogger] = None,
    ):
        self.entity_id = entity_id
        self.creds = creds or {}

        # --------------------------------------------------
        # LOGGER (one file per user)
        # --------------------------------------------------
        self.logger = logger or EntityLogger(
            broker="KITE",
            entity=entity_id,
            component="Adapter"
        )
        self.logger.info("Adapter INIT | Zerodha adapter starting")

        # --------------------------------------------------
        # Credentials
        # --------------------------------------------------
        self.api_key = self.creds.get("ApiKey")
        self.api_secret = self.creds.get("ApiSecret")
        self.user_id = self.creds.get("UserId")
        self.password = self.creds.get("Password")
        self.totp_secret = self.creds.get("TotpSecret")

        # --------------------------------------------------
        # Redis & Formatter
        # --------------------------------------------------
        self.redis_client = RedisClient()
        self.formatter = MessageFormatter(
            tpoms_name="ZERODHA",
            entity_id=self.entity_id
        )

        # --------------------------------------------------
        # Order caches
        # --------------------------------------------------
        self.blitz_to_zerodha: Dict[str, str] = {}
        self.zerodha_to_blitz: Dict[str, str] = {}
        self.order_request_data: Dict[str, Dict] = {}

        # --------------------------------------------------
        # AUTO LOGIN
        # --------------------------------------------------
        self.logger.info("LOGIN_START | Starting Zerodha login")

        self.access_token, error = get_access_token(
            api_key=self.api_key,
            api_secret=self.api_secret,
            user_id=self.user_id,
            password=self.password,
            totp_secret=self.totp_secret,
        )

        if not self.access_token:
            self.logger.error(f"LOGIN_FAILED | {error}")
            raise RuntimeError(f"Zerodha login failed: {error}")

        self.logger.info(f"LOGIN_SUCCESS | Access token received: {self.access_token}")

        # --------------------------------------------------
        # APIs
        # --------------------------------------------------
        self.order_api = ZerodhaOrderAPI(
            api_key=self.api_key,
            access_token=self.access_token
        )

        # --------------------------------------------------
        # Instruments
        # --------------------------------------------------
        try:
            self.instruments_api = ZerodhaInstruments(logger=self.logger)
            self.instruments_api.download_instruments()
            self.instruments_api.load_instruments()
            self.logger.info("INSTRUMENTS_READY | Instruments loaded")
        except Exception as e:
            self.logger.warning(f"INSTRUMENTS_FAILED | {e}")
            self.instruments_api = None

        # --------------------------------------------------
        # WebSocket (TPOMS controlled)
        # --------------------------------------------------
        self.ws: Optional[ZerodhaWebSocket] = None

        self.logger.info("Adapter READY | Zerodha adapter initialized")

    # ============================================================
    # WebSocket (called by TPOMS)
    # ============================================================
    def _start_websocket(self):
        if self.ws:
            self.logger.info("WS_RESTART | Stopping old WebSocket")
            self.ws.stop()
            self.ws = None

        self.logger.info("WS_START | Starting Zerodha WebSocket")

        self.ws = ZerodhaWebSocket(
            api_key=self.api_key,
            access_token=self.access_token,
            user_id=self.user_id,
            redis_client=self.redis_client,
            formatter=self.formatter,
            request_data_mapping=self.order_request_data,
            logger=self.logger,
        )

        self.ws.start()

    def stop(self):
        if self.ws:
            self.ws.stop()
            self.ws = None  # Ensure clean state for reconnect
        self.logger.info("Adapter STOP | Zerodha adapter stopped")

    # ============================================================
    # Command processing
    # ============================================================
    def process_command(self, payload: Dict[str, Any]):
        action = payload.get("Action")
        blitz_data = payload.get("Data")

        self.logger.info(
            "COMMAND | action=%s payload=%s",
            action,
            blitz_data
        )

        if action == "PLACE_ORDER":
            self._handle_place_order(blitz_data)

        elif action == "MODIFY_ORDER":
            self._handle_modify_order(blitz_data)

        elif action == "CANCEL_ORDER":
            self._handle_cancel_order(blitz_data)

        else:
            self.logger.warning(f"COMMAND_UNSUPPORTED | action={action}")

    # ============================================================
    # PLACE ORDER
    # ============================================================
    def _handle_place_order(self, blitz_data: Dict[str, Any]):
        blitz_id = blitz_data.get("BlitzAppOrderID")

        # --------------------------------------------------
        # TEMP CACHE
        # --------------------------------------------------
        if blitz_id:
            temp_key = f"blitz_{blitz_id}"
            self.order_request_data[temp_key] = blitz_data
            self.logger.info(
                "CACHE_TEMP | blitz_id=%s",
                blitz_id
            )

        try:
            params = ZerodhaMapper.to_zerodha(
                blitz_data,
                instruments_api=self.instruments_api
            )

            # Zerodha Rest request
            self.logger.info(
                "ZERODHA_REST_OUT | payload=%s",
                params
            )

            response = self.order_api.place_order(**params)

            # Zerodha Rest response
            self.logger.info(
                "ZERODHA_REST_IN | payload=%s",
                response
            )

            if response.get("status") == "error":
                order_log = ZerodhaMapper.error_to_orderlog(
                    response.get("message"),
                    blitz_data,
                    self.entity_id
                )
                self._publish_order_update(order_log)
                return

            order_id = response["data"]["order_id"]

            self.blitz_to_zerodha[str(blitz_id)] = str(order_id)
            self.zerodha_to_blitz[str(order_id)] = str(blitz_id)

            self.order_request_data[str(order_id)] = blitz_data
            self.order_request_data.pop(f"blitz_{blitz_id}", None)

            # ===================================================
            # >>> ADDED: replay early WS update (race fix)
            # ===================================================
            if self.ws and hasattr(self.ws, "pending_ws_updates"):
                with self.ws.pending_lock:
                    pending = self.ws.pending_ws_updates.pop(str(order_id), None)
                    if pending:
                        self.logger.info(
                            "WS_REPLAY | order_id=%s",
                            order_id
                        )
                        # Call _on_order_update outside the lock to avoid deadlock
                if pending:
                    self.ws._on_order_update(None, pending)
            # ===================================================

            self.logger.info(
                "ORDER_PLACED | blitz=%s zerodha=%s",
                blitz_id,
                order_id
            )

        except Exception as e:
            self.logger.error(
                f"PLACE_FAILED | {e}",
                exc_info=True
            )

            order_log = ZerodhaMapper.error_to_orderlog(
                str(e),
                blitz_data,
                self.entity_id
            )
            self._publish_order_update(order_log)

    # ============================================================
    # MODIFY ORDER
    # ============================================================
    def _handle_modify_order(self, blitz_data: Dict[str, Any]):
        original_request = None
        try:
            zerodha_order_id = blitz_data.get("ExchangeOrderID")
            if not zerodha_order_id:
                raise RuntimeError("Missing ExchangeOrderID")

            original_request = self.order_request_data.get(str(zerodha_order_id))
            if not original_request:
                raise RuntimeError(
                    f"No cached request for order {zerodha_order_id}"
                )

            modify_params = {}

            if blitz_data.get("ModifiedOrderQuantity") is not None:
                modify_params["quantity"] = int(
                    blitz_data["ModifiedOrderQuantity"]
                )

            if blitz_data.get("ModifiedLimitPrice") is not None:
                modify_params["price"] = float(
                    blitz_data["ModifiedLimitPrice"]
                )

            if blitz_data.get("ModifiedStopPrice") is not None:
                modify_params["trigger_price"] = float(
                    blitz_data["ModifiedStopPrice"]
                )

            if blitz_data.get("ModifiedTimeInForce"):
                modify_params["validity"] = ZerodhaMapper._map_validity(
                    blitz_data["ModifiedTimeInForce"]
                )

            if blitz_data.get("ModifiedOrderType"):
                modify_params["order_type"] = ZerodhaMapper._map_order_type(
                    blitz_data["ModifiedOrderType"]
                )

            if blitz_data.get("ModifiedDisclosedQuantity") is not None:
                modify_params["disclosed_quantity"] = int(
                    blitz_data["ModifiedDisclosedQuantity"]
                )

            if not modify_params:
                raise ValueError("No modifiable fields")

            # Zerodha Modify request
            self.logger.info(
                "ZERODHA_MODIFY_REQUEST | order_id=%s payload=%s",
                zerodha_order_id,
                modify_params
            )

            response = self.order_api.modify_order(
                order_id=zerodha_order_id,
                **modify_params
            )

            # Zerodha Modify response
            self.logger.info(
                "ZERODHA_MODIFY_RESPONSE | order_id=%s payload=%s",
                zerodha_order_id,
                response
            )

            if isinstance(response, dict) and response.get("status") == "error":
                order_log = ZerodhaMapper.error_to_orderlog_modify_rejected(
                    response.get("message"),
                    original_request,
                    self.entity_id
                )
                self._publish_order_update(order_log)
                return

            original_request.update(modify_params)
            self.order_request_data[str(zerodha_order_id)] = original_request

            self.logger.info(
                "ORDER_MODIFIED | blitz=%s zerodha=%s",
                original_request.get("BlitzAppOrderID"),
                zerodha_order_id
            )

        except Exception as e:
            self.logger.error(
                f"MODIFY_FAILED | {e}",
                exc_info=True
            )

            order_log = ZerodhaMapper.error_to_orderlog_modify_rejected(
                str(e),
                original_request,
                self.entity_id
            )
            self._publish_order_update(order_log)

    # ============================================================
    # CANCEL ORDER
    # ============================================================
    def _handle_cancel_order(self, blitz_data: Dict[str, Any]):
        try:
            zerodha_order_id = self.blitz_to_zerodha.get(
                str(blitz_data.get("BlitzAppOrderID"))
            )

            if not zerodha_order_id:
                raise RuntimeError("Order ID mapping not found")

            original_request = self.order_request_data.get(str(zerodha_order_id))

            # Zerodha Cancel request
            self.logger.info(
                "ZERODHA_CANCEL_REQUEST | order_id=%s",
                zerodha_order_id
            )

            response = self.order_api.cancel_order(zerodha_order_id)

            # Zerodha Cancel response
            self.logger.info(
                "ZERODHA_CANCEL_RESPONSE | order_id=%s payload=%s",
                zerodha_order_id,
                response
            )

            if response.get("status") == "error":
                order_log = ZerodhaMapper.error_to_orderlog_cancel_rejected(
                    response.get("message"),
                    original_request,
                    self.entity_id
                )
                self._publish_order_update(order_log)
                return

            self.logger.info(
                "ORDER_CANCELLED | blitz=%s zerodha=%s",
                blitz_data.get("BlitzAppOrderID"),
                zerodha_order_id
            )

        except Exception as e:
            self.logger.error(
                f"CANCEL_FAILED | {e}",
                exc_info=True
            )

            order_log = ZerodhaMapper.error_to_orderlog_cancel_rejected(
                str(e),
                self.order_request_data.get(
                    self.blitz_to_zerodha.get(
                        str(blitz_data.get("BlitzAppOrderID"))
                    )
                ),
                self.entity_id
            )
            self._publish_order_update(order_log)

    # ============================================================
    # Publish (TPOMS-compatible)
    # ============================================================
    def _publish_order_update(self, order_log):
        message = self.formatter.order_update(order_log)
        self.redis_client.publish(message)

    @property
    def websocket(self):
        return self.ws
