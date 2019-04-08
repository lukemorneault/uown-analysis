
import pandas as pd
import numpy as np
def ez_boi():
#set pandas display options
    pd.set_option('display.max_columns', 500)

    pd.set_option('display.width', 1000)

def read_and_clean():

    creek_df = pd.read_csv('Stream-map.csv')
    flow_df = pd.read_csv('usgs_mido_month_converted.csv')
    uown_df = pd.read_csv('UOWN_data_master_03212018_adj.csv')

    uown_df = uown_df[uown_df.ID != '520A']
    uown_df = uown_df[uown_df.ID != '520C']
    uown_df = uown_df.dropna(subset=['ID'])

    uown_df['ID'] = uown_df['ID'].astype('int64')
    uown_df['e.coli.cfu'] = uown_df['e.coli.cfu'].astype('float')
    
    kitty = pd.merge(uown_df, creek_df, on=['WS', 'ID'])

    result = pd.merge(kitty, flow_df, on=['month', 'year'])

    return result

def all_by_creek(metric='CON_MEAN'):
    cp_df = f_uown_df.copy()

    cp_df['CON_MEAN'] = cp_df.groupby('Stream WS')['conductivity.uscm'].transform(np.mean)
    cp_df['CON_MED'] = cp_df.groupby('Stream WS')['conductivity.uscm'].transform(np.median)
    cp_df['TUR_MEAN'] = cp_df.groupby('Stream WS')['turbidity.ntu'].transform(np.mean)
    cp_df['TUR_MED'] = cp_df.groupby('Stream WS')['turbidity.ntu'].transform(np.median)
    cp_df['NO3_MEAN'] = cp_df.groupby('Stream WS')['no3.mgL'].transform(np.mean)
    cp_df['NO3_MED'] = cp_df.groupby('Stream WS')['no3.mgL'].transform(np.median)
    cp_df['PO4_MEAN'] = cp_df.groupby(['Stream WS'])['po4.mgL'].transform(np.mean)
    cp_df['PO4_MED'] = cp_df.groupby(['Stream WS'])['po4.mgL'].transform(np.median)
    cp_df['pH_MEAN'] = cp_df.groupby('Stream WS')['pH'].transform(np.mean)
    cp_df['pH_MED'] = cp_df.groupby('Stream WS')['pH'].transform(np.median)
    cp_df['EC_MEAN'] = cp_df.groupby('Stream WS')['e.coli.cfu'].transform(np.mean)
    cp_df['EC_MED'] = cp_df.groupby('Stream WS')['e.coli.cfu'].transform(np.median)
    if metric is 'TUR_MEAN':
        cp_df = cp_df[['Stream WS', 'TUR_MEAN', 'TUR_MED', 'NO3_MEAN', 'NO3_MED', 'pH_MEAN', 'pH_MED', 'EC_MEAN', 'EC_MED']]
    elif metric is 'NO3_MEAN':
        cp_df = cp_df[['Stream WS', 'NO3_MEAN', 'NO3_MED', 'pH_MEAN', 'pH_MED', 'EC_MEAN', 'EC_MED']]
    elif metric is 'PO4_MEAN':
        cp_df = cp_df[['Stream WS', 'PO4_MEAN', 'PO4_MED', 'pH_MEAN', 'pH_MED', 'EC_MEAN', 'EC_MED']]
    elif metric is 'BS_MEAN':
        cp_df = cp_df[['Stream WS', 'BS_MEAN', 'BS_MED']]
    elif metric is 'EC_MEAN':
        cp_df = cp_df[['Stream WS', 'EC_MEAN', 'EC_MED']]
    else:
        cp_df = cp_df[['Stream WS', 'CON_MEAN', 'CON_MED', 'TUR_MEAN', 'TUR_MED', 'NO3_MEAN', 'NO3_MED', 'pH_MEAN', 'pH_MED',
             'EC_MEAN', 'EC_MED', 'BS_MEAN', 'BS_MED']]

    cp_df = cp_df.drop_duplicates()
    cp_df = cp_df.sort_values(by=[metric], ascending=False)
    print
    print cp_df.head(10)


def all_by_year():
    cp_df = f_uown_df.copy()

    cp_df['CON_MEAN'] = cp_df.groupby('year')['conductivity.uscm'].transform(np.mean)
    cp_df['CON_MED'] = cp_df.groupby('year')['conductivity.uscm'].transform(np.median)
    cp_df['TUR_MEAN'] = cp_df.groupby('year')['turbidity.ntu'].transform(np.mean)
    cp_df['TUR_MED'] = cp_df.groupby('year')['turbidity.ntu'].transform(np.median)
    cp_df['NO3_MEAN'] = cp_df.groupby('year')['no3.mgL'].transform(np.mean)
    cp_df['NO3_MED'] = cp_df.groupby('year')['no3.mgL'].transform(np.median)
    cp_df['pH_MEAN'] = cp_df.groupby('year')['pH'].transform(np.mean)
    cp_df['pH_MED'] = cp_df.groupby('year')['pH'].transform(np.median)
    cp_df['EC_MEAN'] = cp_df.groupby('year')['e.coli.cfu'].transform(np.mean)
    cp_df['EC_MED'] = cp_df.groupby('year')['e.coli.cfu'].transform(np.median)
    cp_df = cp_df[['year', 'CON_MEAN', 'CON_MED', 'TUR_MEAN', 'TUR_MED', 'NO3_MEAN', 'NO3_MED', 'pH_MEAN', 'pH_MED', 'EC_MEAN', 'EC_MED']]
    cp_df = cp_df.drop_duplicates()
    cp_df = cp_df.sort_values(by=['CON_MEAN'], ascending=False)
    print cp_df

def all_by_season(metric='CON_MEAN'):
    cp_df = f_uown_df.copy()

    cp_df['CON_MEAN'] = cp_df.groupby('quarter')['conductivity.uscm'].transform(np.mean)
    cp_df['CON_MED'] = cp_df.groupby('quarter')['conductivity.uscm'].transform(np.median)
    cp_df['TUR_MEAN'] = cp_df.groupby('quarter')['turbidity.ntu'].transform(np.mean)
    cp_df['TUR_MED'] = cp_df.groupby('quarter')['turbidity.ntu'].transform(np.median)
    cp_df['NO3_MEAN'] = cp_df.groupby('quarter')['no3.mgL'].transform(np.mean)
    cp_df['NO3_MED'] = cp_df.groupby('quarter')['no3.mgL'].transform(np.median)
    cp_df['pH_MEAN'] = cp_df.groupby('quarter')['pH'].transform(np.mean)
    cp_df['pH_MED'] = cp_df.groupby('quarter')['pH'].transform(np.median)
    cp_df['EC_MEAN'] = cp_df.groupby('quarter')['e.coli.cfu'].transform(np.mean)
    cp_df['EC_MED'] = cp_df.groupby('quarter')['e.coli.cfu'].transform(np.median)
    cp_df = cp_df[['quarter', 'CON_MEAN', 'CON_MED', 'TUR_MEAN', 'TUR_MED', 'NO3_MEAN', 'NO3_MED', 'pH_MEAN', 'pH_MED', 'EC_MEAN', 'EC_MED']]
    cp_df = cp_df.drop_duplicates()
    cp_df = cp_df.sort_values(by=[metric], ascending=False)
    print cp_df

def all_by_month(metric='BS_MEAN'):
    cp_df = f_uown_df.copy()

    cp_df['CON_MEAN'] = cp_df.groupby('month')['conductivity.uscm'].transform(np.mean)
    cp_df['CON_MED'] = cp_df.groupby('month')['conductivity.uscm'].transform(np.median)
    cp_df['TUR_MEAN'] = cp_df.groupby('month')['turbidity.ntu'].transform(np.mean)
    cp_df['TUR_MED'] = cp_df.groupby('month')['turbidity.ntu'].transform(np.median)
    cp_df['BS_MEAN'] = cp_df.groupby('month')['biological_score'].transform(np.mean)
    cp_df['BS_MED'] = cp_df.groupby('month')['biological_score'].transform(np.median)
    cp_df['NO3_MEAN'] = cp_df.groupby('month')['no3.mgL'].transform(np.mean)
    cp_df['NO3_MED'] = cp_df.groupby('month')['no3.mgL'].transform(np.median)
    cp_df['pH_MEAN'] = cp_df.groupby('month')['pH'].transform(np.mean)
    cp_df['pH_MED'] = cp_df.groupby('month')['pH'].transform(np.median)
    cp_df['EC_MEAN'] = cp_df.groupby('month')['e.coli.cfu'].transform(np.mean)
    cp_df['EC_MED'] = cp_df.groupby('month')['e.coli.cfu'].transform(np.median)
    cp_df['FLOW_MEAN'] = cp_df.groupby('month')['39184_00060'].transform(np.mean)
    cp_df['FLOW_MED'] = cp_df.groupby('month')['39184_00060'].transform(np.median)
    cp_df = cp_df[['month', 'CON_MEAN', 'CON_MED', 'TUR_MEAN', 'TUR_MED', 'BS_MEAN', 'BS_MED', 'NO3_MEAN', 'NO3_MED', 'pH_MEAN', 'pH_MED', 'EC_MEAN', 'EC_MED', 'FLOW_MEAN', 'FLOW_MED']]
    cp_df = cp_df.drop_duplicates()
    cp_df = cp_df.sort_values(by=[metric], ascending=False)
    print cp_df

def all_by_site(metric='CON_MEAN'):
    cp_df = f_uown_df.copy()

    cp_df['BS_MEAN'] = cp_df.groupby(['WS', 'ID'])['biological_score'].transform(np.mean)
    cp_df['BS_MED'] = cp_df.groupby(['WS', 'ID'])['biological_score'].transform(np.median)
    cp_df['CON_MEAN'] = cp_df.groupby(['WS', 'ID'])['conductivity.uscm'].transform(np.mean)
    cp_df['CON_MED'] = cp_df.groupby(['WS', 'ID'])['conductivity.uscm'].transform(np.median)
    cp_df['TUR_MEAN'] = cp_df.groupby(['WS', 'ID'])['turbidity.ntu'].transform(np.mean)
    cp_df['TUR_MED'] = cp_df.groupby(['WS', 'ID'])['turbidity.ntu'].transform(np.median)
    cp_df['NO3_MEAN'] = cp_df.groupby(['WS', 'ID'])['no3.mgL'].transform(np.mean)
    cp_df['NO3_MED'] = cp_df.groupby(['WS', 'ID'])['no3.mgL'].transform(np.median)
    cp_df['PO4_MEAN'] = cp_df.groupby(['WS', 'ID'])['po4.mgL'].transform(np.mean)
    cp_df['PO4_MED'] = cp_df.groupby(['WS', 'ID'])['po4.mgL'].transform(np.median)
    cp_df['pH_MEAN'] = cp_df.groupby(['WS', 'ID'])['pH'].transform(np.mean)
    cp_df['pH_MED'] = cp_df.groupby(['WS', 'ID'])['pH'].transform(np.median)
    cp_df['EC_MEAN'] = cp_df.groupby(['WS', 'ID'])['e.coli.cfu'].transform(np.mean)
    cp_df['EC_MED'] = cp_df.groupby(['WS', 'ID'])['e.coli.cfu'].transform(np.median)
    if metric is 'TUR_MEAN':
        cp_df = cp_df[['WS', 'ID', 'Stream', 'TUR_MEAN', 'TUR_MED', 'NO3_MEAN', 'NO3_MED', 'pH_MEAN', 'pH_MED', 'EC_MEAN', 'EC_MED']]
    elif metric is 'NO3_MEAN':
        cp_df = cp_df[['WS', 'ID','Stream', 'NO3_MEAN', 'NO3_MED', 'pH_MEAN', 'pH_MED', 'EC_MEAN', 'EC_MED']]
    elif metric is 'PO4_MEAN':
        cp_df = cp_df[['WS', 'ID', 'Stream', 'PO4_MEAN', 'PO4_MED', 'pH_MEAN', 'pH_MED', 'EC_MEAN', 'EC_MED']]
    elif metric is 'BS_MEAN':
        cp_df = cp_df[['WS', 'ID', 'Stream', 'BS_MEAN', 'BS_MED']]
    elif metric is 'EC_MEAN':
        cp_df = cp_df[['WS', 'ID', 'Stream', 'EC_MEAN', 'EC_MED']]
    else:
        cp_df = cp_df[['WS', 'ID', 'Stream', 'CON_MEAN', 'CON_MED', 'TUR_MEAN', 'TUR_MED', 'NO3_MEAN', 'NO3_MED', 'pH_MEAN', 'pH_MED',
             'EC_MEAN', 'EC_MED', 'BS_MEAN', 'BS_MED']]
    cp_df = cp_df.drop_duplicates()
    cp_df = cp_df.sort_values(by=[metric], ascending=False)
    print
    print cp_df.head(10)

f_uown_df = read_and_clean()
ez_boi()
all_by_creek('EC_MEAN')
##all_by_year()
## all_by_season()
#all_by_month('FLOW_MEAN')
all_by_site('BS_MEAN')
