from sqlalchemy import create_engine

class Read_metrics:

    def __init__(self) -> None:
        
        # db connection
        self.schema_name = "data_enrichment_v3"
        self.engine = create_engine("postgresql://postgres:!vbPostgres@virtualb-rds-data-enrichment.clg6weaheijj.eu-south-1.rds.amazonaws.com:5432/postgres")


    def run(self):
        
        query = f'''
        SELECT target, model, parameters, train_mae, train_mse, train_r2,
        test_mae, test_mse, test_r2, validation_mae, validation_mse, validation_r2,
        execution_date
        
        FROM {self.schema_name}.regression_metrics
        '''

        data = self.engine.execute(statement=query).fetchall()

        models = []
        for model in data:
            sample_properties = {
                "target" : model[0],
                "model" : model[1],
                "paratemers" : model[2],
                "train_mae" : model[3],
                "train_mse" : model[4],
                "train_r2" : model[5],
                "test_mae" : model[6],
                "test_mse" : model[7],
                "test_r2" : model[8],
                "validation_mae" : model[9],
                "validation_mse" : model[10],
                "validation_r2" : model[11],
                "execution_date" : model[12]
            }
            models.append(sample_properties)
            
        return models
