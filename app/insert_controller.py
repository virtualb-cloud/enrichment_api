
class Insert_controller:

    def __init__(self) -> None:

        # object to control the sample
        # before adding to enrichment db

        pass
    
    def first_keys_controller(self, samples:list):
        
        keys = ["sample_source", "sample_date", "sample"]

        for sample in samples:

            # flag True means to have necessary input data

            for key in keys:
                if not key in sample.keys():
                    samples.remove(sample)
                    
        return samples

    def second_keys_controller(self, people:list):

        keys = ["sociodemographics", "status", "cultures", "attitudes", "needs"]
        to_be_deleted = []
        for person in people:

            # flag True means to have necessary input data
            flag = True

            # check if necessary keys exist
            if not "sociodemographics" in person.keys(): flag = False
                
            for key in person.keys():
                if not key in keys: to_be_deleted.append(key)
            
            for key in to_be_deleted:
                del person[key]

            if (len(person) in [0, 1]) | (flag == False): people.remove(person)

        return people      
        
    def sociodemographics_keys_controller(self, people:list):
        
        # mandatory variables
        mandatory_keys = [
            "age", "gender", "location", "profession", "education"
        ]

        # empty lists to gather processable data
        processable_people = []

        for person in people:

            # flag True means to have necessary input data
            flag = True

            # check if necessary keys exist
            for key in mandatory_keys :
                if not key in person["sociodemographics"].keys(): 
                    flag = False
                    break

            # if flag
            if flag == True: processable_people.append(person)

        return processable_people

    def sociodemographics_values_controller(self, people:list):
        
        # functional limits to each variable
        age_limits = [18, 100]

        gender_limits = ["m", "f"]

        region_limits = [
            'piemonte', "valle daosta", 'lombardia', 'trentino alto adige', 'veneto', 'friuli venezia giulia',
            'liguria', 'emilia romagna', 'toscana', 'umbria', 'marche', 'lazio', 'abruzzo', 'molise', 'campania',
            'puglia', 'basilicata', 'calabria', 'sicilia', 'sardegna' 
        ]

        profession_limits = [
            "lavoratore dipendente", "lavoratore indipendente", "pensionato",
            "non occupato"
        ]

        education_limits = [
            "scuola primaria", "scuola secondaria di I grado", "scuola secondaria di II grado",
            "istruzione superiore università", "master di II livello e PHD"
        ]

        # empty lists to gather processable data 
        processable_people = []

        for person in people:

            # flag True means value of all keys are accepted
            flag = True

            # check if necessary variables exist
            if (person["sociodemographics"]["age"] > 100) | (person["sociodemographics"]["age"] < 18): flag = False
            if not person["sociodemographics"]["gender"] in gender_limits: flag = False
            if not person["sociodemographics"]["location"] in region_limits: flag = False
            if not person["sociodemographics"]["profession"] in profession_limits: flag = False
            if not person["sociodemographics"]["education"] in education_limits: flag = False

            # if flag
            if flag == True: processable_people.append(person)

        return processable_people

    def cultures_controller(self, people:list):
        
        # optional variables
        optional_keys = [
            "financial_horizon_index", "finacial_littercay_index", 
            "financial_experience_index", "objective_risk_propensity_index", 
            "subjective_risk_propensity_index", "esg_propensity_index",
            "life_quality_index"
        ]

        # check optional keys and values
        for person in people:

            to_be_deleted = []
            for key in person["cultures"].keys() :
                if not key in optional_keys: to_be_deleted.append(key)
                elif (person["cultures"][key] < 0) | (person["cultures"][key] > 1): to_be_deleted.append(key)
                else: person["cultures"][key] = round(person["cultures"][key], 2)
            for key in to_be_deleted:
                del person["cultures"][key]

            if len(person["cultures"]) == 0: del person["cultures"]

        return people

    def status_controller(self, people:list):
        
        # optional variables
        optional_keys = [
            "net_income_index", "net_expences_index", "net_savings_index",
            "real_assets_index", "financial_assets_index", "net_liabilities_index",
            "net_wealth_index"
        ]


        # check optional keys and values
        for person in people:

            to_be_deleted = []
            for key in person["status"].keys() :
                if not key in optional_keys: to_be_deleted.append(key)
                elif (person["status"][key] < 0) | (person["status"][key] > 1): to_be_deleted.append(key)
                else: person["status"][key] = round(person["status"][key], 2)
            for key in to_be_deleted:
                del person["status"][key]

            if len(person["status"]) == 0: del person["status"]

        return people
            
    def attitudes_controller(self, people:list):
        
        # optional variables
        optional_keys = [
            "bank_activity_index", "digital_activity_index", 
            "cultural_activity_index", "charity_activity_index"
        ]

        # check optional keys and values
        for person in people:
            
            to_be_deleted = []
            for key in person["attitudes"].keys() :
                if not key in optional_keys: to_be_deleted.append(key)
                elif (person["attitudes"][key] < 0) | (person["attitudes"][key] > 1): to_be_deleted.append(key)
                else: person["attitudes"][key] = round(person["attitudes"][key], 2)
            for key in to_be_deleted:
                del person["attitudes"][key]

            if len(person["attitudes"]) == 0: del person["attitudes"]

        return people

    def needs_controller(self, people:list):
        
        # optional variables
        optional_keys = [
            "capital_accumulation_investment_need", "capital_protection_investment_need", 
            "liquidity_investment_need", "income_investment_need",
            "retirement_investment_need", "heritage_investment_need",
            "health_incurance_need", "home_insurance_need",
            "longterm_care_insurance_need", "payment_financing_need",
            "loan_financing_need", "mortgage_financing_need"
        ]

        # check optional keys and values
        for person in people:

            to_be_deleted = []
            for key in person["needs"].keys() :
                if not key in optional_keys: to_be_deleted.append(key)
                elif (person["needs"][key] < 0) | (person["needs"][key] > 1): to_be_deleted.append(key)
                else: person["needs"][key] = round(person["needs"][key], 2)
            for key in to_be_deleted:
                del person["needs"][key]

            if len(person["needs"]) == 0: del person["needs"]

        return people

    def run(self, body:list):

        new_samples = []
        new_people = []

        samples = self.first_keys_controller(body)
        if samples == []: return False, 422, "first-keys-controller"

        for sample in samples:

            people = sample["sample"]

            for person in people:
                
                flags = {
                "status" : False,
                "cultures" : False,
                "attitudes" : False,
                "needs" : False
                } 
                if "cultures" in person.keys(): flags["cultures"] = True
                if "status" in person.keys(): flags["status"] = True
                if "attitudes" in person.keys(): flags["attitudes"] = True
                if "needs" in person.keys(): flags["needs"] = True
                   
                person = self.second_keys_controller([person])
                if person == []: return False, 422, "second-keys-controller"

                person = self.sociodemographics_keys_controller(person)
                if person == []: return False, 422, "sociodemographics-keys-controller"

                person = self.sociodemographics_values_controller(person)
                if person == []: return False, 422, "sociodemographics-values-controller"

                if flags["cultures"]:
                    person = self.cultures_controller(person)
                    if person == []: return False, 422, "cultures-controller"

                if flags["status"]:
                    person = self.status_controller(person)
                    if person == []: return False, 422, "status-controller"
                
                if flags["attitudes"]:
                    person = self.attitudes_controller(person)
                    if person == []: return False, 422, "attitudes-controller"

                if flags["needs"]:
                    person = self.needs_controller(person)
                    if person == []: return False, 422, "needs-controller"
                
                new_people.append(person[0])

            sample["sample"] = new_people
            new_samples.append(sample)

        return True, 200, new_samples