import pkg_resources, shutil, os


def _load_credentials():
    cred_file = pkg_resources.resource_filename(__name__, "data/credential.json")
    shutil.copy(cred_file, ".")
    token_file = pkg_resources.resource_filename(__name__, "data/token.json")
    shutil.copy(token_file, ".")


def _clear_credentials():
    os.remove("credentials.json")
    os.remove("token.json")
