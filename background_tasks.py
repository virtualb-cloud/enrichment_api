from modules.train_models import Train
from modules.insert_samples import Insert
from modules.delete_samples import Delete

def deleter(body:list):
        deleter = Delete()
        response = deleter.run(body=body)
        if response: return True
        else: return False

def trainer(body:dict):
    
    trainer = Train()
    response = trainer.run(body=body)
    if response: return True
    else: return False

def inserter(samples:list):
    
    inserter = Insert()
    response = inserter.run(body=samples)
    if response: return True
    else: return False