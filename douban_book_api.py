from douban_client.api.error import DoubanAPIError
import requests
import simplejson
from douban_client import DoubanClient

__author__ = 'owen2785'



baseurl = 'https://api.douban.com/v2/book/isbn/'


def getbyisbn_without_auth(isbn):
    r = requests.get(baseurl+str(isbn))
    return r.json()