"""
Common Request Handler for TPOMS
Handles receiving, parsing, and validating Blitz TPOMS requests
"""
import json
from typing import Dict, Optional, Any, Tuple


class BlitzRequest:
    """Represents a parsed Blitz TPOMS request"""
    
    def __init__(self, raw_data: str = None, payload: Dict = None):
        """
        Initialize BlitzRequest from raw JSON string or parsed dict
        
        """
        if raw_data:
            try:
                self.payload = json.loads(raw_data)
            except json.JSONDecodeError as e:
                print(f"Failed to parse JSON: {e}")
                raise ValueError(f"Invalid JSON format: {e}")
        elif payload:
            self.payload = payload
        else:
            raise ValueError("Either raw_data or payload must be provided")
        
        # Extract common fields
        self.action = self.payload.get("Action") 
        self.tpoms_name = self.payload.get("TPOmsName") 
        self.user_id = self.payload.get("UserId")
        self.user_name = self.payload.get("UserName")
        self.data = self.payload.get("Data")
        
        # Parse Data if it's a JSON string
        if isinstance(self.data, str):
            try:
                self.data = json.loads(self.data)
            except json.JSONDecodeError:
                # If parsing fails, keep as string (for credential data)
                pass
    
    def validate(self) -> Tuple[bool, Optional[str]]:
        """
        Validate that required fields are present
        
        """
        if not self.action:
            return False, "Missing required field: Action"
        
        if not self.tpoms_name:
            return False, "Missing required field: TPOmsName"
        
        if not self.user_id:
            return False, "Missing required field: UserId"
        
        return True, None
    
   
    
class RequestHandler:
    """Common request handler for processing Blitz TPOMS requests"""
    
    @staticmethod
    def parse_request(raw_data: str) -> Optional[BlitzRequest]:
        """
        Parse raw Redis message into BlitzRequest
        
        """
        try:
            request = BlitzRequest(raw_data=raw_data)
            is_valid, error_msg = request.validate()
            
            if not is_valid:
                print(f"Invalid request: {error_msg}")
                return None
            
            print(f"Parsed request: Action={request.action}, TPOmsName={request.tpoms_name}, UserId={request.user_id}")
            return request
            
        except ValueError as e:
            print(f"Failed to parse request: {e}")
            return None
        except Exception as e:
            print(f"Unexpected error parsing request: {e}")
            return None
    
    @staticmethod
    def extract_credentials(data: Any) -> Optional[Dict]:
        """
        Extract credentials from Data field
        """
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except json.JSONDecodeError:
                return None
        
        if isinstance(data, list):
            # Convert list of {Name, Value} pairs to dict
            creds = {}
            for item in data:
                if isinstance(item, dict):
                    name = item.get("Name") or item.get("name")
                    value = item.get("Value") or item.get("value")
                    if name and value:
                        creds[name] = value
            return creds if creds else None
        
        if isinstance(data, dict):
            return data
        
        return None
    import json

    def extract_api_error(api_error):
        try:
            if isinstance(api_error, Exception):
                api_error = str(api_error)

            data = json.loads(api_error)

            return {
                "status": data.get("status"),
                "message": data.get("message"),
                "errorcode": data.get("errorcode"),
                "uniqueorderid": data.get("uniqueorderid"),
            }

        except Exception:
            # fallback if error is not JSON
            return {
                "status": "ERROR",
                "message": str(api_error),
                "errorcode": None,
                "uniqueorderid": None,
            }
