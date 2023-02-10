

class Predict_controller:

    def __init__(self) -> None:

        # mandatory and optional input variables
        self.mandatory_keys = [
            "id", "age", "gender", "location", "profession", "education"
        ]

        # functional limits to each variable
        self.age_limits = [0, 120]

        self.gender_limits = ["m", "f", "o"]

        self.region_limits = [
            'piemonte', "valle daosta", 'lombardia', 'trentino alto adige', 'veneto', 'friuli venezia giulia',
            'liguria', 'emilia romagna', 'toscana', 'umbria', 'marche', 'lazio', 'abruzzo', 'molise', 'campania',
            'puglia', 'basilicata', 'calabria', 'sicilia', 'sardegna' 
        ]

        self.profession_limits = [
            "lavoratore dipendente", "lavoratore indipendente", "pensionato",
            "non occupato"
        ]

        self.education_limits = [
            "scuola primaria", "scuola secondaria di I grado", "scuola secondaria di II grado",
            "istruzione superiore università", "master di II livello e PHD"
        ]
        
    def keys_controller(self, configurations:list):
        
        # empty lists to gather processable data and unprocessable data
        processable_data = []
        unprocessable_data = []

        for config in configurations:

            # flag True means to have necessary input data
            flag = True

            # check if necessary keys exist
            for key in self.mandatory_keys :
                if not key in config.keys(): flag = False

            # if flag is still True, clean data and add to "processable_data" dict
            if flag == True:
                config = {
                    "id" : config["id"],
                    "age" : config["age"],
                    "gender" : config["gender"],
                    "location" : config["location"],
                    "profession" : config["profession"],
                    "education" : config["education"],
                }
                processable_data.append(config)

            else:
                unprocessable_data.append(config)

        return processable_data, unprocessable_data

    def values_controller(self, configurations:list):
        
        # empty lists to gather processable data and unprocessable data
        processable_data = []
        unprocessable_data = []

        for config in configurations:

            # flag True means value of all keys are accepted
            flag = True

            # check if necessary variables exist
            if (config["age"] > 120) | (config["age"] < 0): flag = False
            if not config["gender"] in self.gender_limits: flag = False
            if not config["location"] in self.region_limits: flag = False
            if not config["profession"] in self.profession_limits: flag = False
            if not config["education"] in self.education_limits: flag = False

            if flag == True:
                processable_data.append(config)

            else:
                unprocessable_data.append(config)

        return processable_data, unprocessable_data

    def run(self, configurations:list):

        # wanrning notes
        key_errors = {}
        value_errors = {}

        # 1) controll the keys
        valid_configurations, invalid_configurations_former = self.keys_controller(configurations)

        # gather the ids of invalid configurations
        if invalid_configurations_former != []:
            
            invalid_ids = []

            for config in invalid_configurations_former:
                invalid_ids.append(config["id"])

            key_errors["discarded_ids"] = invalid_ids
            key_errors["note"] = f'''IDs are discarded due to lack of necessary input keys,
                please consider sending all the following variables for all records: {self.mandatory_keys}'''

        # 2) controll the values
        valid_configurations, invalid_configurations_latter = self.values_controller(valid_configurations)
        
        # gather the ids of invalid configurations
        if invalid_configurations_latter != []:
            
            invalid_ids = []

            for config in invalid_configurations_latter:
                invalid_ids.append(config["id"])

                value_errors["discarded_ids"] = invalid_ids
                value_errors["note"]  = f'''IDs are discarded due to unrecognised values to keys, please consider sending values regarding each key correctly for all records:
                    age: 0 yrs <= age <= 120 yrs, gender: {self.gender_limits}, 
                    profession: {self.profession_limits}, education: {self.education_limits}, location: {self.region_limits}
                    '''
        errors = {"key_errors" : key_errors, "value_errors" : value_errors}

        return valid_configurations, errors