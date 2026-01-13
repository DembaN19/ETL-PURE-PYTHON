from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import padding
from hashlib import sha256
import os
from functools import wraps
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Fonction pour déchiffrer une valeur
def decrypt_value(encrypted_value, key):
    """
    Déchiffre une valeur avec AES-256-CBC.
    """
    if isinstance(key, str):
        key = key.encode('utf-8')

    key = sha256(key).digest()
    iv, encrypted = encrypted_value.split(':')
    iv = bytes.fromhex(iv)
    encrypted_data = bytes.fromhex(encrypted)

    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    decryptor = cipher.decryptor()

    # Déchiffrement des données
    decrypted_padded = decryptor.update(encrypted_data) + decryptor.finalize()
    unpadder = padding.PKCS7(128).unpadder()
    decrypted = unpadder.update(decrypted_padded) + unpadder.finalize()

    return decrypted.decode('utf-8')

# Décorateur pour déchiffrer automatiquement les paramètres
def decrypt_params(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        secret_key = os.getenv("SECRET_KEY")
        new_args = []
        new_kwargs = {}

        # Déchiffrement des arguments positionnels
        for arg in args:
            if isinstance(arg, str) and ':' in arg:  # On suppose que les params chiffrés contiennent ':'
                try:
                    new_args.append(decrypt_value(arg, secret_key))
                except Exception:
                    new_args.append(arg)
            else:
                new_args.append(arg)

        # Déchiffrement des arguments nommés
        for key, value in kwargs.items():
            if isinstance(value, str) and ':' in value:
                try:
                    new_kwargs[key] = decrypt_value(value, secret_key)
                except Exception:
                    new_kwargs[key] = value
            else:
                new_kwargs[key] = value

        # Appel de la fonction avec les paramètres déchiffrés
        return func(*new_args, **new_kwargs)
    return wrapper

