import requests
import jwt
import time

class Auth:
    def __init__(self, base_url, realm, client_id, client_secret):
        self.token_url = f"{base_url}/realms/{realm}/protocol/openid-connect/token"
        self.client_id = client_id
        self.client_secret = client_secret
        self.decoded_token = ""
        self.token = ""
        self.public_key = None

    def get_token(self, username, password):
        data = {
            "grant_type": "password",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "username": username,
            "password": password,
            "scope": "openid profile email"
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        response = requests.post(self.token_url, data=data, headers=headers)
        if response.status_code == 200:
            self.token = response.json()
            self.decoded_token = self.__decode_jwt(self.token)
            return {"token": self.token, "decoded_token": self.decoded_token}
        else:
            raise Exception(f"Error {response.status_code}: {response.text}")

    def __decode_jwt(self, token):
        try:
            # Si el token es un diccionario, extraemos el 'access_token'
            if isinstance(token, dict):
                token = token.get("access_token", "")
            
            # Convertimos a string si es necesario
            token = str(token)
            
            # Decodificamos el token
            return jwt.decode(token, options={"verify_signature": False})
        except Exception as e:
            raise Exception(f"Error al decodificar el token: {str(e)}")
  
    def validate_token(self, token):

        try:
            # Si no has cargado la clave pública, obténla desde Keycloak
            if self.public_key is None:
                self.public_key = self.__get_public_key()

            # Decodificamos y validamos la firma
            decoded_token = jwt.decode(token, self.public_key, algorithms=["RS256"], audience=self.client_id)

            # Verificamos si el token está expirado
            if decoded_token["exp"] < time.time():
                raise Exception("El token ha expirado")
            
            return decoded_token  # Token válido

        except jwt.ExpiredSignatureError:
            raise Exception("El token ha expirado")
        except jwt.InvalidTokenError as e:
            raise Exception(f"Token inválido: {str(e)}")

    def __get_public_key(self):
        # Obtiene la clave pública de Keycloak.
        key_url = f"{self.token_url}/certs"
        response = requests.get(key_url)
        if response.status_code == 200:
            certs = response.json()
            # Asumiendo que el JWT usa RS256, obtén la clave pública del primer certificado
            public_key = certs["keys"][0]["x5c"][0]
            return f"-----BEGIN CERTIFICATE-----\n{public_key}\n-----END CERTIFICATE-----"
        else:
            raise Exception(f"Error al obtener la clave pública: {response.status_code}")

auth = Auth("http://localhost:8280", "vault-realm", "pepe", "VowinZxLmK2xkZhVj3jM0ouVRpMXdiYk")
token_response = auth.get_token("perico", "sabbath")
print(token_response)

try:
    valid_token = auth.validate_token(token = token_response["token"]["access_token"])
    print("Token válido:", valid_token)
except Exception as e:
    print("Error de validación:", e)