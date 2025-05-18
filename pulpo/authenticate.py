import requests
import jwt
import time
from cryptography.x509 import load_pem_x509_certificate
from cryptography.hazmat.backends import default_backend

class Auth:
    def __init__(self, base_url, realm, client_id, client_secret):
        self.token_url = f"{base_url}/realms/{realm}/protocol/openid-connect/token"
        self.cert_url = f"{base_url}/realms/{realm}/protocol/openid-connect/certs"
        self.client_id = client_id
        self.client_secret = client_secret
        self.decoded_token = ""
        self.token = ""
        self.public_key = None
        self.otp = None

    def login(self, username, password, otp_code = None):
        data = {
            "grant_type": "password",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "username": username,
            "password": password,
            "scope": "openid profile email",
        }

        # Si se proporciona un código OTP, lo añadimos a los datos de la solicitud
        self.otp = otp_code
        if otp_code:
            data['totp'] = otp_code

        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        response = requests.post(self.token_url, data=data, headers=headers)
        if response.status_code == 200:
            access_token = response.json().get("access_token")
            self.token = access_token
            self.decoded_token = self.__decode_jwt(access_token)
            return {
                "access_token": self.token,
                "decoded_token": self.decoded_token
            }
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
        response = requests.get(self.cert_url)
        if response.status_code == 200:
            certs = response.json()
            cert_str = certs["keys"][0]["x5c"][0]
            cert_pem = f"-----BEGIN CERTIFICATE-----\n{cert_str}\n-----END CERTIFICATE-----"
            cert_obj = load_pem_x509_certificate(cert_pem.encode(), default_backend())
            public_key = cert_obj.public_key()
            return public_key
        else:
            raise Exception(f"Error al obtener la clave pública: {response.status_code}")

    def exchange_token_from(self, au1: "Auth"):
        '''
        Intercambia un token de acceso por un nuevo token de acceso y refresh.
        El token resultante se almacena en au2.token.
        '''
        if not au1.token:
            raise ValueError("El Auth origen (au1) no tiene un token válido")

        response = requests.post(
            self.token_url,
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
                "subject_token": au1.token,
                "subject_token_type": "urn:ietf:params:oauth:token-type:access_token",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
                # "requested_subject": "opcional-si-haces-impersonation",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

        if response.status_code == 200:
            access_token = response.json().get("access_token")
            if not access_token:
                raise ValueError("No se recibió access_token en la respuesta")

            self.token = access_token
            self.decoded_token = self.__decode_jwt(access_token)
            return {
                "access_token": self.token,
                "decoded_token": self.decoded_token
            }
        else:
            raise RuntimeError(f"❌ Error {response.status_code}: {response.text}")


if __name__ == "__main__":

    auth = Auth("https://seguridad.merocomsolutions.com", "master", "informes", "M5DaowmNZJR4t6MrFxX27Y7CNTyxR0bC")

    # Obtener el token usando nombre de usuario y contraseña
    otp = input("Por favor ingresa tu código OTP (FreeOTP): ")
    token_response = auth.login("dos", "dos",otp)
    print(token_response)

    try:
        # Usamos directamente el campo 'access_token' que devuelve get_token()
        valid_token = auth.validate_token(token=token_response["access_token"])
        print("Token válido:", valid_token)
    except Exception as e:
        print("Error de validación:", e)

    # Creamos otro Auth y usamos el primero para intercambiar su token
    auth2 = Auth("https://seguridad.merocomsolutions.com", "master", "informes", "M5DaowmNZJR4t6MrFxX27Y7CNTyxR0bC")
    token_response2 = auth2.exchange_token_from(auth)
    print(token_response2)

    try:
        # También aquí usamos el 'access_token' devuelto por exchange_token_from()
        valid_token = auth2.validate_token(token=token_response2["access_token"])
        print("Token válido:", valid_token)
    except Exception as e:
        print("Error de validación:", e)
