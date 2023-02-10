from sqlalchemy import create_engine

class Delete:

    def __init__(self) -> None:
        
        # db connection
        self.schema_name = "data_enrichment_v3"
        self.engine = create_engine("postgresql://postgres:!vbPostgres@virtualb-rds-data-enrichment.clg6weaheijj.eu-south-1.rds.amazonaws.com:5432/postgres")

    def check(self, body:dict):
        
        requested_ids = body["ids"]

        query = f'''
        SELECT sample_id 
        FROM {self.schema_name}.sample_properties
        '''

        present_ids_tuple = self.engine.execute(statement=query).fetchall()
        present_ids = []
        for element in present_ids_tuple:
            present_ids.append(element[0])

        final_ids = []
        for id in requested_ids:
            if id in present_ids: 
                if id >= 10: 
                    final_ids.append(id)
        
        if final_ids == []: return False, []
        print(final_ids)
        return True, final_ids
  

    def run(self, body:dict):
        
        response = self.check(body=body)
        if not response[0]: return False, 422

        ids = response[1]

        if len(ids) == 1:

            query = f'''
            DELETE FROM {self.schema_name}.sample_properties as sp
            WHERE sp.sample_id = {ids[0]}
            '''
        elif len(ids) >= 1:

            query = f'''
            DELETE FROM {self.schema_name}.sample_properties as sp
            WHERE sp.sample_id in {tuple(ids)}
            '''

        self.engine.execute(statement=query)
  
        return True, 200
