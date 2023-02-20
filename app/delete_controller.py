
class Delete_controller:

    def __init__(self) -> None:

        # object to control the sample
        # before adding to enrichment db

        pass
    
    def first_keys_controller(self, body:dict):
        
        keys = ["ids"]

        # flag True means to have necessary input data
        if not keys[0] in body.keys(): return False
        else: return True



    def run(self, body:dict):

        response = self.first_keys_controller(body)
        if not response: return False, 422, "first-keys-controller"

        return True, 200, ""