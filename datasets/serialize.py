
from dhis2eo.integrations.pandas import dataframe_to_dhis2_json

from .utils import get_time_dim

def dataframe_to_json_data(df, dataset):
    time_dim = get_time_dim(df)
    varname = dataset['variable']

    # pretend its a dhis2 payload json
    data = dataframe_to_dhis2_json(
        df, 
        data_element_id='dummy', 
        org_unit_col='id', 
        period_col=time_dim, 
        value_col=varname, 
    )['dataValues']

    # but remove the data element id which is not needed
    for item in data:
        del item['dataElement']

    # return
    return data
