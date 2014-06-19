import geventhttpclient.httplib


def patch_all():
    geventhttpclient.httplib.patch()
    patch()


def patch():
    """
      Patch the geventhttpclient to work with urllib3 (used by elasticsearch-py)
    """
    geventhttpclient.httplib.HTTPResponse.length = (
        geventhttpclient.httplib.HTTPResponse.content_length)
