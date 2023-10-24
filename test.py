from fx5 import FX5
import pandas as pd
import numpy as np
host = "10.10.1.14:5000"
name = "mx14"
res={'name':name ,'parts': np.nan ,'value': np.nan , \
		'user_id': np.nan ,'work_order_id': np.nan,'option1':np.nan,'option2':np.nan,'ping':np.nan}

try:
    fx5 = FX5.get_connection(host)
    
    
    res['parts']=fx5.read("D3000",2)

    print(res)
    fx5.close()
    

except Exception as err:
    print("PLC connect err",name, err)
    e = {'name':name,'err': 'PLC ERR ' + str(err) }
    print(e)
    fx5.close()
    pass