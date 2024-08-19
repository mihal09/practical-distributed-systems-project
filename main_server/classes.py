from enum import Enum


class Device(Enum):
    MOBILE = 'MOBILE'
    PC = 'PC'
    TV = 'TV'

class Action(Enum):
    VIEW = 'VIEW'
    BUY = 'BUY'

class Aggregate(Enum):
    COUNT = 'COUNT'
    SUM_PRICE = 'SUM_PRICE'


class Product:
    def __init__(self, product_id, brand_id, category_id, price):
        self.product_id = product_id
        self.brand_id = brand_id
        self.category_id = category_id
        self.price = price

    @classmethod
    def from_json(cls, data):
        return cls(data['product_id'], data['brand_id'], data['category_id'], data['price'])


class UserTagEvent:
    def __init__(self, time, cookie, country, device, action, origin, product_info):
        self.time = datetime.fromisoformat(time)
        self.cookie = cookie
        self.country = country
        self.device = Device[device]
        self.action = Action[action]
        self.origin = origin
        self.product_info = Product.from_json(product_info)
        

class UserProfileResult:
    def __init__(self, cookie, views=None, buys=None):
        self.cookie = cookie
        self.views = views or []
        self.buys = buys or []

class AggregatesQueryResult:
    def __init__(self, columns=None, rows=None):
        self.columns = columns or []
        self.rows = rows or []