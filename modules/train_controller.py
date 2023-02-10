
class Train_controller:

    def __init__(self) -> None:
        

        self.gender_map = {
            "m" : 1,
            "f" : 2
        }
        
        self.location_map = {
            'piemonte': 1,
            'valle daosta': 2,
            'lombardia': 3,
            'trentino alto adige': 4,
            'veneto': 5,
            'friuli venezia giulia': 6,
            'liguria': 7,
            'emilia romagna': 8,
            'toscana': 9,
            'umbria': 10,
            'marche': 11,
            'lazio': 12,           
            'abruzzo': 13,
            'molise': 14,
            'campania': 15,
            'puglia': 16,
            'basilicata': 17,
            'calabria': 18,
            'sicilia': 19,
            'sardegna': 20
        }
        
        self.education_map = {
            "scuola primaria" : 1,
            "scuola secondaria di I grado" : 2,
            "scuola secondaria di II grado" : 3,
            "istruzione superiore università" : 4,
            "master di II livello e PHD" : 5
        }
        
        self.profession_map = {
            "non occupato" : 1,
            "lavoratore dipendente" : 2,
            "lavoratore indipendente" : 3,
            "pensionato" : 4
            
        }
        
    def main_keys_controller(self, body:dict):

        flag = True
        main_keys = ["predict_set"]

        for key in main_keys:
            if not key in body.keys():
                flag = False
                break
        
        return flag

    def keys_controller(self, body:dict):
        
        flag = True
        s_level_keys = ["mean_age", "mode_gender", "mode_location", "mode_education", "mode_profession"]
        
        for key in s_level_keys:
            
            if not key in body["predict_set"].keys():
                flag = False
                break
        
        return flag

    def values_controller(self, body:dict):

        flag = True

        for key, value in body["predict_set"].items():

            if "gender" in key:
                if not value in self.gender_map.keys():
                    flag = False
                    break
            
            if "age" in key:
                if (value < 0) | (value > 120) :
                    flag = False

            if "location" in key:
                if not value in self.location_map.keys():
                    flag = False

            if "education" in key:
                if not value in self.education_map.keys():
                    flag = False

            if "profession" in key:
                if not value in self.profession_map.keys():
                    flag = False

        return flag
            

    def run(self, body:dict):

        response = self.main_keys_controller(body)
        if not response: return False

        response = self.keys_controller(body)
        if not response: return False

        response = self.values_controller(body)
        if not response: return False

        return True
