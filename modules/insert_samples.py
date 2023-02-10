from sqlalchemy import create_engine

class Insert:

    def __init__(self) -> None:

        # db connection
        self.schema_name = "data_enrichment_v3"
        self.engine = create_engine("postgresql://postgres:!vbPostgres@virtualb-rds-data-enrichment.clg6weaheijj.eu-south-1.rds.amazonaws.com:5432/postgres")

        # sample id

        query = f'''
        SELECT max(sample_id)
        FROM {self.schema_name}.sample_properties
        '''
        with self.engine.connect() as conn:
            response = conn.execute(statement=query)
        self.sample_id = response.fetchone()[0]

        if self.sample_id == None: self.sample_id = 0

        # person id

        query = f'''
        SELECT max(person_id)
        FROM {self.schema_name}.sociodemographics
        '''
        with self.engine.connect() as conn:
            response = conn.execute(statement=query)
        self.person_id = response.fetchone()[0]

        if self.person_id == None: self.person_id = 0

    def insert_sample_properties(self, sample:list, sample_date:str, sample_source:str, sample_content:dict):
        
        sample_size = len(sample)
        
        count = 0

        ages = 0
        genders = {}
        locations = {}
        educations = {}
        professions = {}

        for record in sample:

            count += 1
            ages += record["sociodemographics"]["age"]

            if record["sociodemographics"]["gender"] in genders.keys():
                genders[record["sociodemographics"]["gender"]] += 1
            else:
                genders[record["sociodemographics"]["gender"]] = 0

            if record["sociodemographics"]["location"] in locations.keys():
                locations[record["sociodemographics"]["location"]] += 1
            else:
                locations[record["sociodemographics"]["location"]] = 0

            if record["sociodemographics"]["education"] in educations.keys():
                educations[record["sociodemographics"]["education"]] += 1
            else:
                educations[record["sociodemographics"]["education"]] = 0

            if record["sociodemographics"]["profession"] in professions.keys():
                professions[record["sociodemographics"]["profession"]] += 1
            else:
                professions[record["sociodemographics"]["profession"]] = 0

        mean_age = round(ages /count)

        max = 0
        for key, value in genders.items():
            if value >= max: 
                max = value
                mode_gender = key

        max = 0
        for key, value in locations.items():
            if value >= max: 
                max = value
                mode_location = key

        max = 0
        for key, value in educations.items():
            if value >= max: 
                max = value
                mode_education = key

        max = 0
        for key, value in professions.items():
            if value >= max: 
                max = value
                mode_profession = key

        content = '' 
        for key, value in sample_content.items():
            if value: content = content + key + ', '
        
        content = content[:-2] 
        
        query = f'''
        SELECT mean_age, mode_gender, mode_location, mode_education,
        mode_profession, sample_size, sample_date, sample_source, sample_content
        FROM {self.schema_name}.sample_properties
        '''
        with self.engine.connect() as conn:
            data = conn.execute(statement=query).fetchall()
        sample_identity = tuple(
            [mean_age, mode_gender, 
            mode_location, mode_education, mode_profession, 
            sample_size, sample_date, sample_source, content]
        )

        if sample_identity in data: return False

        else:
            query = f'''
            INSERT INTO {self.schema_name}.sample_properties(
                sample_id, mean_age, mode_gender, mode_location, mode_education,
                mode_profession, sample_size, sample_date, sample_source, sample_content
            )
            VALUES (
                {self.sample_id}, {mean_age}, '{mode_gender}', 
                '{mode_location}', '{mode_education}', '{mode_profession}', 
                {sample_size}, '{sample_date}', '{sample_source}', '{content}'
            )
            '''
            with self.engine.connect() as conn:
                conn.execute(statement=query)

            return True

    def insert_people_sociodemographics(self, sample:list):
        
        # prepare fixed query
        query = f'''
            INSERT INTO {self.schema_name}.sociodemographics(
                person_id, sample_id, age_, gender_, location_, education_, profession_
            )
            VALUES 
            '''
        
        # add the data to query
        for person in sample:
            
            record = person["sociodemographics"]

            add_statement = f'''
            (
                {record["person_id"]}, {self.sample_id}, {record["age"]}, '{record["gender"]}',
                '{record["location"]}', '{record["education"]}', '{record["profession"]}'
            ),'''
            query = query + add_statement

        # to exclude the last ","
        query = query[:-1] 

        # execute the query
        with self.engine.connect() as conn:
            conn.execute(statement=query)

        return True

    def insert_people_cultures(self, sample:list):

        # fixed query
        query = f'''
        INSERT INTO {self.schema_name}.cultures(
            person_id, financial_horizon_index, financial_litteracy_index,
            financial_experience_index, objective_risk_propensity_index,
            subjective_risk_propensity_index, esg_propensity_index,
            life_quality_index
        )
        VALUES 
        '''
        
        keys_list = [
            "financial_horizon_index", "financial_litteracy_index",
            "financial_experience_index", "objective_risk_propensity_index",
            "subjective_risk_propensity_index", "esg_propensity_index",
            "life_quality_index"]

        for person in sample:

            # check
            if not "cultures" in person.keys(): continue

            record = person["cultures"]
            
            # control keys
            for key in keys_list:
                if not key in record.keys(): record[key] = "NULL"

            # add the data to query   
            add_statement = f'''
            (
                {record["person_id"]}, {record["financial_horizon_index"]},
                {record["financial_litteracy_index"]}, {record["financial_experience_index"]},
                {record["objective_risk_propensity_index"]}, {record["subjective_risk_propensity_index"]}, 
                {record["esg_propensity_index"]}, {record["life_quality_index"]}
            ),'''

            query = query + add_statement
        
        # to exclude the last ","
        query = query[:-1] 

        # execute the query
        with self.engine.connect() as conn:
            conn.execute(statement=query)

        return True

    def insert_people_status(self, sample:list):

        # fixed query
        query = f'''
        INSERT INTO {self.schema_name}.status(
            person_id, real_assets_index, financial_assets_index,
            net_liabilities_index, net_wealth_index,
            net_income_index, net_savings_index,
            net_expences_index
        )
        VALUES 
        '''

        keys_list = [
            "real_assets_index", "financial_assets_index",
            "net_liabilities_index", "net_wealth_index",
            "net_income_index", "net_savings_index",
            "net_expences_index"
        ]

        for person in sample:

            # check
            if not "status" in person.keys(): continue

            record = person["status"]
            
            # control keys
            for key in keys_list:
                if not key in record.keys(): record[key] = "NULL"

            # add the data to query  
            add_statement = f'''
            (
                {record["person_id"]}, {record["real_assets_index"]},
                {record["financial_assets_index"]}, {record["net_liabilities_index"]},
                {record["net_wealth_index"]}, {record["net_income_index"]}, 
                {record["net_savings_index"]}, {record["net_expences_index"]}
            ),'''

            query = query + add_statement
        
        # to exclude the last ","
        query = query[:-1] 

        # execute the query
        with self.engine.connect() as conn:
            conn.execute(statement=query)

        return True

    def insert_people_attitudes(self, sample:list):
        
        # fixed query
        query = f'''
        INSERT INTO {self.schema_name}.attitudes(
            person_id, "bank_activity_index", "digital_activity_index",
            "cultural_activity_index", "charity_activity_index"
        )
        VALUES 
        '''

        keys_list = [
            "bank_activity_index", "digital_activity_index",
            "cultural_activity_index", "charity_activity_index"
        ]

        for person in sample:

            # check
            if not "attitudes" in person.keys(): continue

            record = person["attitudes"]
            
            # control keys
            for key in keys_list:
                if not key in record.keys(): record[key] = "NULL"

            # add the data to query  
            add_statement = f'''(
                {record["person_id"]},
                {record["bank_activity_index"]}, {record["digital_activity_index"]},
                {record["cultural_activity_index"]}, {record["charity_activity_index"]}
            ),'''

            query = query + add_statement

        # to exclude the last ","
        query = query[:-1] 

        # execute the query
        with self.engine.connect() as conn:
            conn.execute(statement=query)

        return True

    def insert_people_needs(self, sample:list):
        
        # fixed query
        query = f'''
        INSERT INTO {self.schema_name}.needs(
            person_id, health_insurance_need, home_insurance_need, longterm_care_insurance_need, 
            payment_financing_need, loan_financing_need, mortgage_financing_need, 
            capital_accumulation_investment_need, capital_protection_investment_need,
            retirement_investment_need, income_investment_need, 
            heritage_investment_need, liquidity_investment_need
        )
        VALUES 
        '''

        keys_list = [
            'health_insurance_need', 'home_insurance_need', 'longterm_care_insurance_need', 
            'payment_financing_need', 'loan_financing_need', 'mortgage_financing_need', 
            'capital_accumulation_investment_need', 'capital_protection_investment_need',
            'retirement_investment_need', 'income_investment_need', 
            'heritage_investment_need', 'liquidity_investment_need'
        ]

        for person in sample:

            # check
            if not "needs" in person.keys(): continue

            record = person["needs"]
            
            # control keys
            for key in keys_list:
                if not key in record.keys(): record[key] = "NULL"

            # add the data to query 
            add_statement = f'''
            (
                {record["person_id"]}, {record["health_insurance_need"]},
                {record["home_insurance_need"]}, {record["longterm_care_insurance_need"]},
                {record["payment_financing_need"]}, {record["loan_financing_need"]}, 
                {record["mortgage_financing_need"]}, {record["capital_accumulation_investment_need"]},
                {record["capital_protection_investment_need"]},
                {record["retirement_investment_need"]}, {record["income_investment_need"]}, 
                {record["heritage_investment_need"]}, {record["liquidity_investment_need"]}
            ),'''
            query = query + add_statement

        # to exclude the last ","
        query = query[:-1] 

        # execute the query
        with self.engine.connect() as conn:
            conn.execute(statement=query)
        
        return True

    def run(self, body:list):
        
        
        for sample in body:
            
            print(sample.keys())
            self.sample_id += 1

            # get data    
            sample_source = sample["sample_source"]
            sample_date = sample["sample_date"]
            people = sample["sample"]

            flags = {
                "status" : False,
                "cultures" : False,
                "attitudes" : False,
                "needs" : False
            } 

            for person in people:

                self.person_id += 1

                person["sociodemographics"]["person_id"] = self.person_id

                if "cultures" in person.keys(): 
                    flags["cultures"] = True
                    person["cultures"]["person_id"] = self.person_id
                    
                if "status" in person.keys(): 
                    flags["status"] = True
                    person["status"]["person_id"] = self.person_id

                if "attitudes" in person.keys(): 
                    flags["attitudes"] = True
                    person["attitudes"]["person_id"] = self.person_id

                if "needs" in person.keys(): 
                    flags["needs"] = True
                    person["needs"]["person_id"] = self.person_id
                            
            
            # insert sample
            response = self.insert_sample_properties(people, sample_date, sample_source, sample_content=flags)
            if not response: return False
            
            self.insert_people_sociodemographics(sample=people)
            print("sociodemographics pushed")

            if flags["cultures"]: 
                self.insert_people_cultures(sample=people)
                print("cultures pushed")

            if flags["status"]: 
                self.insert_people_status(sample=people)
                print("status pushed")

            if flags["attitudes"]: 
                self.insert_people_attitudes(sample=people)
                print("attitudes pushed")

            if flags["needs"]: 
                self.insert_people_needs(sample=people)
                print("needs pushed")

        return True
