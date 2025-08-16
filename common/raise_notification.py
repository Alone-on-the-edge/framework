from oauthlib.oauth2 import BackendApplicationClient
import logging

def get_oauth_token(token_url, client_id, client_secret, scope, OAuth2Session):
    try:
        logging.debug("Retrieving oauth token")
        client = BackendApplicationClient(client_id=client_id)
        oauth = OAuth2Session(client=client)
        token = oauth.fetch_token(token_url = token_url,
                                  client_id = client_id,
                                  client_secret = client_secret,
                                  scope=scope)
        return token["access_token"]
    except Exception as ex:
        logging.error(f"Error Occured ", exc_info=True)

import json
import requests
def post_msg_to_event_notification(payload, env, args):
    client_id = args[0]
    scope = args[1]
    client_secret = args[2]
    x_api_key = args[3]
    OAuth2Session = args[4]
    if env == "fit":
        endpoint_url = "https://ds-api.us-east1.abc/"
        token_url = "https://apioauthfit.nj.xxx.com/oauth/token"
    elif env == "iat":
        endpoint_url = "https://ds-api.us-east1.abc/"
        token_url = "https://apioauthiat.nj.xxx.com/oauth/token"
    elif env == "prod":
        endpoint_url = "https://ds-api.us-east1.abc/"
        token_url = "https://apioauthprod.nj.xxx.com/oauth/token"
    
    data = payload

    token = get_oauth_token(token_url, client_id, client_secret, scope, OAuth2Session)

    headers = {
        "Content-type" : "application/json",
        "x-api-key" : x_api_key,
        "Authorization" : "{}".format(token)
    }
    print("sending request")
    json_data_bytes = json.dumps(data).encode('utf-8')
    response = requests.post(endpoint_url,
                             json_data_bytes,
                             headers=headers,
                             timeout=20, verify=False)
    
    return response