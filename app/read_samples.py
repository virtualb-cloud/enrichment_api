from sqlalchemy import create_engine

class Read:

    def __init__(self) -> None:
        
        # db connection
        self.schema_name = "data_enrichment_v3"
        self.engine = create_engine("postgresql://postgres:!vbPostgres@virtualb-rds-data-enrichment.clg6weaheijj.eu-south-1.rds.amazonaws.com:5432/postgres")


    def run(self):
        
        query = f'''
        SELECT sample_id, mean_age, mode_gender, mode_location, mode_education,
        mode_profession, sample_size, sample_date, sample_source, sample_content
        
        FROM {self.schema_name}.sample_properties
        '''

        data = self.engine.execute(statement=query).fetchall()

        samples = []
        for sample in data:
            sample_properties = {
                "sample_id" : sample[0],
                "sample_source" : sample[8],
                "sample_date" : sample[7],
                "sample_size" : sample[6],
                "sample_content" : sample[9],
                "mean_age" : sample[1],
                "mode_gender" : sample[2],
                "mode_location" : sample[3],
                "mode_education" : sample[4],
                "mode_profession" : sample[5]
            }
            samples.append(sample_properties)
            
        return samples
