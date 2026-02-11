
import json
import os
import sys
import threading
import signal
import atexit

import config
from common.redis_client import RedisClient
from common.request_handler import RequestHandler
from common.message_formatter import MessageFormatter
from common.broker_order_mapper import OrderLog

#from Motilal.motilal_adapter import MotilalAdapter
from Zerodha.zerodha_adapter import ZerodhaAdapter

from common.logging_setup import EntityLogger

# =============================================================================
# BOOTSTRAP
# =============================================================================

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# =============================================================================
# SYSTEM LOGGER → CONSOLE ONLY
# =============================================================================

# logging.basicConfig(
#     level=logging.INFO,
#     format="[%(asctime)s] %(levelname)-9s [TPOMS] %(message)s",
#     datefmt="%Y-%m-%d %H:%M:%S"
# )

print("TPOMS process starting")

redis_client = RedisClient()

# =============================================================================
# CONSTANTS & REGISTRY
# =============================================================================

BROKER_CONNECTOR_MAP = {

    "KITE": ZerodhaAdapter,
}

connectors = {}
_shutdown_done = False

# =============================================================================
# UTILS
# =============================================================================

def normalize_broker(broker):
    if not broker:
        return None
    broker = broker.upper()
    return broker if broker in BROKER_CONNECTOR_MAP else None


def is_authenticated(connector):
    return connector and (getattr(connector, "access_token", None)
        or getattr(connector, "jwt_token", None)
    )


# =============================================================================
# REDIS RESPONSE PUBLISHER
# =============================================================================

def publish_response(message_type, broker, entity_id, status, message):
    formatter = MessageFormatter(tpoms_name=broker, entity_id=entity_id)

    if message_type in {
        "TPOMS_CONNECT",
        "TPOMS_DISCONNECT",
        "TPOMS_REFRESH",
        "WEBSOCKET_CONNECTED",
        "WEBSOCKET_DISCONNECTED",
    }:
        response = formatter.connection_status(status, message)
    else:
        response = formatter.response(message_type, status, message)

    redis_client.publish(response)


# =============================================================================
# CONNECTOR REGISTRY
# =============================================================================

def _key(broker, entity):
    return f"{broker}:{entity}"


def get_connector(broker, entity):
    return connectors.get(_key(broker, entity))


def add_connector(broker, entity, connector):
    connectors[_key(broker, entity)] = connector


def remove_connector(broker, entity):
    connectors.pop(_key(broker, entity), None)


# =============================================================================
# ADAPTER START
# =============================================================================

def start_adapter(broker, entity_id, creds, action):
    ConnectorClass = BROKER_CONNECTOR_MAP.get(broker)
    if not ConnectorClass:
        return None

    # ------------------------------------------------------------
    # ENTITY LOGGER 
    # ------------------------------------------------------------
    logger = EntityLogger(broker=broker, entity=entity_id, component="Adapter")
    logger.info("Starting adapter")

    connector = ConnectorClass(entity_id=entity_id, creds=creds, logger=logger)

    if not is_authenticated(connector):
        error = getattr(connector, "login_error_message", "Login failed")
        logger.error(error)

        publish_response(action, broker, entity_id, "FAILED", error)

        try:
            connector.stop()
        except Exception:
            pass

        return None

    add_connector(broker, entity_id, connector)

    if hasattr(connector, "_start_websocket"):
        connector._start_websocket()

    publish_response(action, broker, entity_id, "CONNECTED", f"Adapter connected for {broker}:{entity_id}" )

    logger.info("Adapter connected successfully")

    return connector


# =============================================================================
# TPOMS ACTION HANDLERS
# =============================================================================

def handle_refresh(broker, entity_id, connector):
    publish_response(
        "TPOMS_REFRESH",
        broker,
        entity_id,
        "CONNECTED" if connector and is_authenticated(connector) else "DISCONNECTED",
        "Adapter is connected" if connector else "Adapter not connected"
    )


def handle_disconnect(broker, entity_id, connector):
    if connector:
        try:
            connector.stop()
        except Exception:
            pass

        remove_connector(broker, entity_id)

    publish_response(
        "TPOMS_DISCONNECT",
        broker,
        entity_id,
        "DISCONNECTED",
        f"Disconnected {broker}:{entity_id}"
    )


def handle_connect(broker, entity_id, connector, payload):
    if connector and is_authenticated(connector):
        publish_response(
            "TPOMS_CONNECT",
            broker,
            entity_id,
            "CONNECTED",
            "Already connected"
        )
        return

    publish_response(
        "TPOMS_CONNECT",
        broker,
        entity_id,
        "CONNECTING",
        "Connecting to broker..."
    )

    creds = RequestHandler.extract_credentials(payload.get("Data") or {})
    if not creds:
        publish_response(
            "TPOMS_CONNECT",
            broker,
            entity_id,
            "FAILED",
            "Missing credentials"
        )
        return

    start_adapter(broker, entity_id, creds, "TPOMS_CONNECT")


# =============================================================================
# REDIS MESSAGE ROUTER
# =============================================================================

def process_redis_message(raw_data):
    try:
        request = RequestHandler.parse_request(raw_data)
        if not request:
            return
        
        broker = normalize_broker(request.tpoms_name)
        entity_id = request.user_id
        action = request.action
        user_name = request.user_name

        if not broker or not action or (not entity_id and not user_name):
            return

        connector = get_connector(broker, entity_id) if entity_id else None

        if not connector and user_name:
            connector = get_connector(broker, user_name)
            if connector:
                entity_id = user_name

        if action == "TPOMS_REFRESH":
            handle_refresh(broker, entity_id, connector)
            return

        if action == "TPOMS_DISCONNECT":
            handle_disconnect(broker, entity_id, connector)
            return

        if action == "TPOMS_CONNECT":
            handle_connect(broker, entity_id, connector, request.payload)
            return

        if not connector or not is_authenticated(connector):
            blitz_data = request.payload.get("Data") if request.payload else None
            if isinstance(blitz_data, list):
                blitz_data = blitz_data[0]

            order_log = OrderLog.orderlog_error(
                error_msg="Adapter not connected",
                blitz_data=blitz_data,
                err_status="Rejected",
                action=action
            )

            order_log_json = json.dumps(order_log.to_dict())
            redis_client.publish(order_log_json)
            return

        connector.process_command(request.payload)

    except Exception as e:
        logging.exception(str(e))


# =============================================================================
# SHUTDOWN
# =============================================================================

def shutdown():
    global _shutdown_done
    if _shutdown_done:
        return
    _shutdown_done = True

    print("Shutting down TPOMS")

    # Use handle_disconnect for each active connector
    for key in list(connectors.keys()):
        try:
            broker, entity = key.split(":", 1)
            handle_disconnect(broker, entity, connectors.get(key))
        except Exception as e:
            print(f"Failed to disconnect {key}: {e}")


# =============================================================================
# MAIN
# =============================================================================

def main():
    pubsub = redis_client.connection.pubsub()
    pubsub.subscribe(config.CH_BLITZ_REQUESTS)

    print(f"Listening on {config.CH_BLITZ_REQUESTS}")

    signal.signal(signal.SIGTERM, lambda *_: shutdown())
    atexit.register(shutdown)

    for message in pubsub.listen():
        if message["type"] == "message":
            threading.Thread(
                target=process_redis_message,
                args=(message["data"],),
                daemon=True
            ).start()


if __name__ == "__main__":
    main()
