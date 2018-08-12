
#!/usr/bin/env python


#
# FREE CLIMATE DATA
# https://www.ncdc.noaa.gov/cdo-web/
#

import pandas as pd
from pandas.tools.plotting import table
import numpy as np

import matplotlib.pyplot as plt
from datetime import datetime as dt



host = "127.0.0.1"
port = 27017


def print_full(df):
    pd.set_option('display.max_rows', len(df))
    print(df)
    pd.reset_option('display.max_rows')


def set_pd_display_options():
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)


def read_data():

    # read in UOWN data
    uown_df = pd.read_csv('UOWN_data_master_03212018_adj.csv')
    uown_df['ID'] = uown_df['ID'].astype('int64')
    #print uown_df['biological_score'].dtype
    #print uown_df['pH'].dtype
    #print uown_df['conductivity.uscm'].dtype
    #uown_df['biological_score'] = uown_df['biological_score'].astype('int64')

    # add precip data eventually?
    #precip_df = pd.read_csv('ben-epps-airport-precip.csv')
    #print precip_df
    #pdf = pd.merge(uown_df, precip_df, on='year')

    # merge stream name mapping to UOWN sites
    stream_df = pd.read_csv('Stream-map.csv')
    stream_df['ID'] = stream_df['ID'].astype(object)
    df = pd.merge(uown_df, stream_df, on=['WS', 'ID'], how='left')
    print "MERGE WITH STREAM DB"
    print df.shape

    # merge in flow data
    #
    #  USGS (middle oconee) 02217500 Latitude 33o56'48",   Longitude 83o25'22"
    #     Atl Hwy in Athens
    # 
    #  USGS (north oconee) 02217770 Latitude 33o58'11",   Longitude 83o22'39" 
    #     at College Station
    # 
    usgs_df = pd.read_csv('usgs-flow-data.csv')
    final_df = pd.merge(df, usgs_df, on=['WS', 'month', 'year'], how='left')
    print "MERGE WITH FLOW"
    print final_df.shape
    final_df.to_csv("final_csv.csv")
    print
    print "READ DATA COMPLETE"
    return final_df



def clean_data(df):
    # sometimes ID is blank
    df['ID'].replace('', np.nan, inplace=True)
    df.dropna(subset=['ID'], inplace=True)

    # only consider sites that have been samples > 1 time
    df['samples'] = df.groupby(['WS','ID'])['conductivity.uscm'].transform('count')
    print
    print "CLEAN DATA (remove sites sampled only once)"
    print
    #print df[df['samples'] <  2]
    df = df[df['samples'] >  1]
    print
    print "CLEAN DATA DONE"
    print
    return df


#
# provides watershed-year breakdown
#
def evaluate_by_watershed_year():
    st_df = df.copy()


    st_df['cond_med'] = st_df.groupby(['year','WS'])['conductivity.uscm'].transform(np.median)
    st_df['cond_mean'] = st_df.groupby(['year','WS'])['conductivity.uscm'].transform(np.mean)

    st_df['turb_med'] = st_df.groupby(['year','WS'])['turbidity.ntu'].transform(np.median)
    st_df['turb_mean'] = st_df.groupby(['year','WS'])['turbidity.ntu'].transform(np.mean)

    st_df = st_df[['WS', 'year', 'cond_mean', 'cond_med', 'turb_mean', 'turb_med']]
    st_df = st_df.drop_duplicates()
    st_df = st_df.sort_values('cond_mean',ascending=False)
    print st_df.head(20)

    #st_df.plot.scatter(x='year', y='cond_mean')
    fig, ax = plt.subplots()

    colors = {'MIDO':'red', 'BICO':'blue', 'NORO':'green'}

    #for key, grp in st_df.groupby(['WS']):
        #ax = grp.plot(ax=ax, kind='scatter', x='year', y='cond_mean', c=colors[key], label=key)

    #plt.show()

    for key, grp in st_df.groupby(['WS']):
        ax = grp.plot(ax=ax, kind='scatter', x='year', y='turb_mean', c=colors[key], label=key)

    plt.show()

    st_df = st_df.sort_values('turb_mean',ascending=False)
    print st_df.head(20)


#
# provides year-qtr breakdown
#
def evaluate_by_qtr_year():
    st_df = df.copy()


    st_df['cond_med'] = st_df.groupby(['year','quarter'])['conductivity.uscm'].transform(np.median)
    st_df['cond_mean'] = st_df.groupby(['year','quarter'])['conductivity.uscm'].transform(np.mean)

    st_df['turb_med'] = st_df.groupby(['year','quarter'])['turbidity.ntu'].transform(np.median)
    st_df['turb_mean'] = st_df.groupby(['year','quarter'])['turbidity.ntu'].transform(np.mean)

    st_df = st_df[['quarter', 'year', 'cond_mean', 'cond_med', 'turb_mean', 'turb_med']]
    st_df = st_df.drop_duplicates()
    st_df = st_df.sort_values('cond_mean',ascending=False)
    print st_df.head(20)

    st_df = st_df.sort_values('turb_mean',ascending=False)
    print st_df.head(20)


#
# provides season (qtr) breakdown
#
def evaluate_by_season():
    st_df = df.copy()


    st_df['cond_med'] = st_df.groupby(['quarter'])['conductivity.uscm'].transform(np.median)
    st_df['cond_mean'] = st_df.groupby(['quarter'])['conductivity.uscm'].transform(np.mean)

    st_df['turb_med'] = st_df.groupby(['quarter'])['turbidity.ntu'].transform(np.median)
    st_df['turb_mean'] = st_df.groupby(['quarter'])['turbidity.ntu'].transform(np.mean)
    st_df['no3_mean'] = st_df.groupby(['quarter'])['no3.mgL'].transform(np.mean)
    st_df['pH_mean'] = st_df.groupby(['quarter'])['pH'].transform(np.mean)
    st_df['EC_mean'] = st_df.groupby(['quarter'])['e.coli.cfu'].transform(np.mean)

    #st_df['flow_mean'] = st_df.groupby(['quarter'])['39184_00060'].transform(np.mean)
    #st_df['flow_med'] = st_df.groupby(['quarter'])['39184_00060'].transform(np.median)
    st_df['flow_mean'] = st_df.groupby(['quarter'])['Flow'].transform(np.mean)
    st_df['flow_med'] = st_df.groupby(['quarter'])['Flow'].transform(np.median)


    st_df = st_df[['quarter', 'cond_mean', 'cond_med', 'turb_mean', 'turb_med', 'no3_mean', 'pH_mean', 'EC_mean', 'flow_mean']]
    st_df = st_df.drop_duplicates()
    st_df = st_df.sort_values('cond_mean',ascending=False)
    print
    print "Seasonal by Conductivity Mean"
    print
    print st_df.head(20)

    st_df = st_df.sort_values('turb_mean',ascending=False)
    print
    print "Seasonal by Turbidity Mean"
    print
    print st_df.head(20)

#
# provides year breakdown
#
def evaluate_by_year():
    st_df = df.copy()


    st_df['cond_med'] = st_df.groupby(['year'])['conductivity.uscm'].transform(np.median)
    st_df['cond_mean'] = st_df.groupby(['year'])['conductivity.uscm'].transform(np.mean)

    st_df['turb_med'] = st_df.groupby(['year'])['turbidity.ntu'].transform(np.median)
    st_df['turb_mean'] = st_df.groupby(['year'])['turbidity.ntu'].transform(np.mean)
    st_df['no3_mean'] = st_df.groupby(['year'])['no3.mgL'].transform(np.mean)
    st_df['pH_mean'] = st_df.groupby(['year'])['pH'].transform(np.mean)
    st_df['EC_mean'] = st_df.groupby(['year'])['e.coli.cfu'].transform(np.mean)

    #st_df['flow_mean'] = st_df.groupby(['year'])['39184_00060'].transform(np.mean)
    #st_df['flow_med'] = st_df.groupby(['year'])['39184_00060'].transform(np.median)
    st_df['flow_mean'] = st_df.groupby(['year'])['Flow'].transform(np.mean)
    st_df['flow_med'] = st_df.groupby(['year'])['Flow'].transform(np.median)


    st_df = st_df[['year', 'cond_mean', 'cond_med', 'turb_mean', 'turb_med', 'no3_mean', 'pH_mean', 'EC_mean', 'flow_mean']]
    st_df = st_df.drop_duplicates()
    st_df = st_df.sort_values('cond_mean',ascending=False)
    print
    print "Year by Conductivity Mean"
    print
    print st_df

    st_df = st_df.sort_values('flow_mean',ascending=False)
    print
    print "Year by Flow Mean"
    print
    print st_df

    st_df = st_df.sort_values('year',ascending=False)
    print
    print "By Year"
    print
    print st_df


#
# provides year breakdown for a site
#
def evaluate_site_by_year(ws, id):
    st_df = df.copy()


    st_df['cond_med'] = st_df.groupby([ws][id]['year'])['conductivity.uscm'].transform(np.median)
    st_df['cond_mean'] = st_df.groupby([ws][id]['year'])['conductivity.uscm'].transform(np.mean)

    st_df['turb_med'] = st_df.groupby([ws][id]['year'])['turbidity.ntu'].transform(np.median)
    st_df['turb_mean'] = st_df.groupby([ws][id]['year'])['turbidity.ntu'].transform(np.mean)
    st_df['no3_mean'] = st_df.groupby([ws][id]['year'])['no3.mgL'].transform(np.mean)
    st_df['pH_mean'] = st_df.groupby([ws][id]['year'])['pH'].transform(np.mean)
    st_df['EC_mean'] = st_df.groupby([ws][id]['year'])['e.coli.cfu'].transform(np.mean)


    st_df = st_df[['year', 'cond_mean', 'cond_med', 'turb_mean', 'turb_med', 'no3_mean', 'pH_mean', 'EC_mean']]
    st_df = st_df.drop_duplicates()
    print
    print "Site by Year"
    print
    print st_df


#
# provides month breakdown
#
def evaluate_by_month():
    st_df = df.copy()


    st_df['csamples'] = st_df.groupby(['month'])['conductivity.uscm'].transform('count')
    st_df['tsamples'] = st_df.groupby(['month'])['turbidity.ntu'].transform('count')
    st_df['nsamples'] = st_df.groupby(['month'])['no3.mgL'].transform('count')
    st_df['psamples'] = st_df.groupby(['month'])['pH'].transform('count')
    st_df['esamples'] = st_df.groupby(['month'])['e.coli.cfu'].transform('count')
    st_df['bsamples'] = st_df.groupby(['month'])['biological_score'].transform('count')

    st_df['cond_med'] = st_df.groupby(['month'])['conductivity.uscm'].transform(np.median)
    st_df['cond_mean'] = st_df.groupby(['month'])['conductivity.uscm'].transform(np.mean)

    st_df['turb_med'] = st_df.groupby(['month'])['turbidity.ntu'].transform(np.median)
    st_df['turb_mean'] = st_df.groupby(['month'])['turbidity.ntu'].transform(np.mean)
    st_df['no3_mean'] = st_df.groupby(['month'])['no3.mgL'].transform(np.mean)
    st_df['pH_mean'] = st_df.groupby(['month'])['pH'].transform(np.mean)
    st_df['EC_mean'] = st_df.groupby(['month'])['e.coli.cfu'].transform(np.mean)
    st_df['BS_mean'] = st_df.groupby(['month'])['biological_score'].transform(np.mean)
    st_df['BS_med'] = st_df.groupby(['month'])['biological_score'].transform(np.median)

    #st_df['flow_mean'] = st_df.groupby(['month'])['39184_00060'].transform(np.mean)
    #st_df['flow_med'] = st_df.groupby(['month'])['39184_00060'].transform(np.median)
    st_df['flow_mean'] = st_df.groupby(['month'])['Flow'].transform(np.mean)
    st_df['flow_med'] = st_df.groupby(['month'])['Flow'].transform(np.median)

    #st_df['month'] = st_df['month'].astype('int64')

    con_df = st_df[['month', 'cond_mean', 'cond_med', 'csamples', 'flow_mean', 'flow_med']]
    con_df = con_df.drop_duplicates()
# figure out sample size for each
    #st_df['samples'] = st_df.groupby(['month']).transform('count')
    con_df = con_df.sort_values('cond_mean',ascending=False)
    print
    print "Monthly by Conductivity Mean"
    print
    print con_df.head(40)

    tur_df = st_df[['month', 'turb_mean', 'turb_med', 'tsamples', 'flow_mean', 'flow_med']]
    tur_df = tur_df.drop_duplicates()
    tur_df = tur_df.sort_values('turb_mean',ascending=False)
    print
    print "Monthly by Turbidity Mean"
    print
    print tur_df.head(40)

    bs_df = st_df[['month', 'BS_mean', 'BS_med', 'bsamples', 'flow_mean', 'flow_med']]
    bs_df = bs_df.drop_duplicates()
    bs_df = bs_df.sort_values('BS_mean',ascending=False)
    print
    print "Monthly by Biological Mean"
    print
    print bs_df.head(20)


#
# provides month-watershed breakdown
#
def evaluate_by_month_watershed():
    st_df = df.copy()


    st_df['csamples'] = st_df.groupby(['WS', 'month'])['conductivity.uscm'].transform('count')
    st_df['tsamples'] = st_df.groupby(['WS', 'month'])['turbidity.ntu'].transform('count')
    st_df['nsamples'] = st_df.groupby(['WS', 'month'])['no3.mgL'].transform('count')
    st_df['psamples'] = st_df.groupby(['WS', 'month'])['pH'].transform('count')
    st_df['esamples'] = st_df.groupby(['WS', 'month'])['e.coli.cfu'].transform('count')
    st_df['bsamples'] = st_df.groupby(['WS', 'month'])['biological_score'].transform('count')

    st_df['cond_med'] = st_df.groupby(['WS', 'month'])['conductivity.uscm'].transform(np.median)
    st_df['cond_mean'] = st_df.groupby(['WS', 'month'])['conductivity.uscm'].transform(np.mean)

    st_df['turb_med'] = st_df.groupby(['WS', 'month'])['turbidity.ntu'].transform(np.median)
    st_df['turb_mean'] = st_df.groupby(['WS', 'month'])['turbidity.ntu'].transform(np.mean)
    st_df['no3_mean'] = st_df.groupby(['WS', 'month'])['no3.mgL'].transform(np.mean)
    st_df['no3_med'] = st_df.groupby(['WS', 'month'])['no3.mgL'].transform(np.median)
    st_df['pH_mean'] = st_df.groupby(['WS', 'month'])['pH'].transform(np.mean)
    st_df['EC_mean'] = st_df.groupby(['WS', 'month'])['e.coli.cfu'].transform(np.mean)
    st_df['BS_mean'] = st_df.groupby(['WS', 'month'])['biological_score'].transform(np.mean)
    st_df['BS_med'] = st_df.groupby(['WS', 'month'])['biological_score'].transform(np.median)

    #st_df['flow_mean'] = st_df.groupby(['WS', 'month'])['39184_00060'].transform(np.mean)
    #st_df['flow_med'] = st_df.groupby(['WS', 'month'])['39184_00060'].transform(np.median)
    st_df['flow_mean'] = st_df.groupby(['WS', 'month'])['Flow'].transform(np.mean)
    st_df['flow_med'] = st_df.groupby(['WS', 'month'])['Flow'].transform(np.median)

    #st_df['month'] = st_df['month'].astype('int64')

    con_df = st_df[['WS', 'month', 'cond_mean', 'cond_med', 'csamples', 'flow_mean', 'flow_med']]
    con_df = con_df.drop_duplicates()
# figure out sample size for each
    #st_df['samples'] = st_df.groupby(['month']).transform('count')
    con_df = con_df.sort_values('cond_med',ascending=False)
    print
    print "MIDO Monthly by Conductivity Median"
    print
    print con_df[con_df['WS'] == 'MIDO'].head(40)
    print
    print "NORO Monthly by Conductivity Median"
    print
    print con_df[con_df['WS'] == 'NORO'].head(40)

    tur_df = st_df[['WS', 'month', 'turb_mean', 'turb_med', 'tsamples', 'flow_mean', 'flow_med']]
    tur_df = tur_df.drop_duplicates()
    tur_df = tur_df.sort_values('turb_med',ascending=False)
    print
    print "MIDO Monthly by Turbidity Median"
    print
    print tur_df[tur_df['WS'] == 'MIDO'].head(40)
    print
    print "NORO Monthly by Turbidity Median"
    print
    print tur_df[tur_df['WS'] == 'NORO'].head(40)

    bs_df = st_df[['WS', 'month', 'BS_mean', 'BS_med', 'bsamples', 'flow_mean', 'flow_med']]
    bs_df = bs_df.drop_duplicates()
    bs_df = bs_df.sort_values('BS_mean',ascending=False)
    print
    print "MIDO Monthly by Biological Mean"
    print
    print bs_df[bs_df['WS'] == 'MIDO'].head(40)
    print
    print "NORO Monthly by Biological Mean"
    print
    print bs_df[bs_df['WS'] == 'NORO'].head(40)

    bs_df = st_df[['WS', 'month', 'no3_mean', 'no3_med', 'bsamples', 'flow_mean', 'flow_med']]
    bs_df = bs_df.drop_duplicates()
    bs_df = bs_df.sort_values('no3_mean',ascending=False)
    print
    print "MIDO Monthly by NO3 Mean"
    print
    print bs_df[bs_df['WS'] == 'MIDO'].head(40)
    print
    print "NORO Monthly by NO3 Mean"
    print
    print bs_df[bs_df['WS'] == 'NORO'].head(40)



#
# provides month-year breakdown
#
def evaluate_by_month_year():
    st_df = df.copy()


    st_df['cond_med'] = st_df.groupby(['month','year'])['conductivity.uscm'].transform(np.median)
    st_df['cond_mean'] = st_df.groupby(['month','year'])['conductivity.uscm'].transform(np.mean)

    st_df['turb_med'] = st_df.groupby(['month','year'])['turbidity.ntu'].transform(np.median)
    st_df['turb_mean'] = st_df.groupby(['month','year'])['turbidity.ntu'].transform(np.mean)
    st_df['no3_mean'] = st_df.groupby(['month','year'])['no3.mgL'].transform(np.mean)
    st_df['pH_mean'] = st_df.groupby(['month','year'])['pH'].transform(np.mean)
    st_df['EC_mean'] = st_df.groupby(['month','year'])['e.coli.cfu'].transform(np.mean)
    st_df['BS_mean'] = st_df.groupby(['month','year'])['biological_score'].transform(np.mean)
    st_df['BS_med'] = st_df.groupby(['month','year'])['biological_score'].transform(np.median)

    #st_df['flow_mean'] = st_df.groupby(['month','year'])['39184_00060'].transform(np.mean)
    #st_df['flow_med'] = st_df.groupby(['month','year'])['39184_00060'].transform(np.median)
    st_df['flow_mean'] = st_df.groupby(['month','year'])['Flow'].transform(np.mean)
    st_df['flow_med'] = st_df.groupby(['month','year'])['Flow'].transform(np.median)

    st_df = st_df[['month', 'year', 'cond_mean', 'cond_med', 'turb_mean', 'turb_med', 'no3_mean', 'pH_mean', 'EC_mean', 'BS_mean', 'BS_med', 'flow_mean', 'flow_med']]
    st_df = st_df.drop_duplicates()
    st_df['month'] = st_df['month'].astype('int64')

# figure out sample size for each
    #st_df['samples'] = st_df.groupby(['month']).transform('count')
    st_df = st_df.sort_values('turb_med',ascending=False)
    print
    print "Monthly by Turb Median"
    print
    print st_df
#
# provides watershed breakdown
#
def evaluate_by_watershed():
    st_df = df.copy()


    st_df['cond_med'] = st_df.groupby(['WS'])['conductivity.uscm'].transform(np.median)
    st_df['cond_mean'] = st_df.groupby(['WS'])['conductivity.uscm'].transform(np.mean)
    st_df['cond_max'] = st_df.groupby('WS')['conductivity.uscm'].transform('max')

    st_df['turb_med'] = st_df.groupby(['WS'])['turbidity.ntu'].transform(np.median)
    st_df['turb_mean'] = st_df.groupby(['WS'])['turbidity.ntu'].transform(np.mean)

    st_df['no3_mean'] = st_df.groupby(['WS'])['no3.mgL'].transform(np.mean)
    st_df['pH_mean'] = st_df.groupby(['WS'])['pH'].transform(np.mean)
    st_df['EC_mean'] = st_df.groupby(['WS'])['e.coli.cfu'].transform(np.mean)
    st_df['BS_mean'] = st_df.groupby(['WS'])['biological_score'].transform(np.mean)
    st_df['BS_med'] = st_df.groupby(['WS'])['biological_score'].transform(np.median)

    #st_df = st_df[['WS', 'cond_mean', 'cond_med', 'turb_mean', 'turb_med']]
    st_df = st_df[['WS', 'cond_mean', 'cond_med', 'cond_max', 'turb_mean', 'turb_med', 'no3_mean', 'EC_mean', 'BS_mean', 'pH_mean']]
    st_df = st_df.drop_duplicates()
    print
    print "The Three Watersheds"
    print
    st_df = st_df.sort_values('cond_mean',ascending=False)
    print st_df


#
# provides stream breakdown
#
def evaluate_by_stream_watershed(stream):
    st_df = df.copy()


    if (stream != "All"):
        st_df = st_df[st_df['Stream WS'] == stream]

    st_df['cond_med'] = st_df.groupby(['Stream WS'])['conductivity.uscm'].transform(np.median)
    st_df['cond_mean'] = st_df.groupby(['Stream WS'])['conductivity.uscm'].transform(np.mean)
    st_df['cond_max'] = st_df.groupby(['Stream WS'])['conductivity.uscm'].transform('max')

    st_df['turb_med'] = st_df.groupby(['Stream WS'])['turbidity.ntu'].transform(np.median)
    st_df['turb_mean'] = st_df.groupby(['Stream WS'])['turbidity.ntu'].transform(np.mean)

    st_df['no3_mean'] = st_df.groupby(['Stream WS'])['no3.mgL'].transform(np.mean)
    st_df['no3_med'] = st_df.groupby(['Stream WS'])['no3.mgL'].transform(np.median)
    st_df['pH_mean'] = st_df.groupby(['Stream WS'])['pH'].transform(np.mean)
    st_df['EC_mean'] = st_df.groupby(['Stream WS'])['e.coli.cfu'].transform(np.mean)

    #st_df = st_df[['Stream WS', 'cond_mean', 'cond_med', 'turb_mean', 'turb_med', 'PRCP']]
    st_df = st_df[['Stream WS', 'WS', 'cond_mean', 'cond_med', 'cond_max', 'turb_mean', 'turb_med', 'no3_mean', 'no3_med', 'pH_mean', 'EC_mean']]
    st_df = st_df.drop_duplicates()
    st_df = st_df.sort_values('cond_mean',ascending=False)
    print
    print "TOP 15 by mean Conductivity"
    print
    print st_df.head(15)

    st_df = st_df.sort_values('EC_mean',ascending=False)
    print
    print "TOP 15 by mean EC"
    print
    print st_df.head(10)

    # set fig size
    fig, ax = plt.subplots(figsize=(12, 3)) 
    # no axes
    ax.xaxis.set_visible(False)  
    ax.yaxis.set_visible(False)  
    # no frame
    ax.set_frame_on(False)  
    # plot table
    tab = table(ax, st_df, loc='upper right')  
    # set font manually
    tab.auto_set_font_size(False)
    tab.set_fontsize(8) 
    # save the result
    plt.savefig('table.png')

    st_df = st_df.sort_values('EC_mean',ascending=False)
    print
    print "TOP 15 by mean E.coli"
    print
    print st_df.head(15)

    st_df = st_df.sort_values('no3_mean',ascending=False)
    print
    print "TOP 15 by mean NO3"
    print
    print st_df.head(15)

    st_df = st_df.sort_values('turb_mean',ascending=False)
    print
    print "TOP 15 by mean Turbidity"
    print
    print st_df.head(15)


#
# provides stream breakdown by year
#
def evaluate_by_stream_watershed_year(stream_ws):
    st_df = df.copy()


    if (stream_ws != "All"):
        st_df = st_df[st_df['Stream WS'] == stream_ws]

    st_df['cond_med'] = st_df.groupby(['Stream WS','year'])['conductivity.uscm'].transform(np.median)
    st_df['cond_mean'] = st_df.groupby(['Stream WS','year'])['conductivity.uscm'].transform(np.mean)
    st_df['cond_max'] = st_df.groupby(['Stream WS','year'])['conductivity.uscm'].transform('max')

    st_df['turb_med'] = st_df.groupby(['Stream WS','year'])['turbidity.ntu'].transform(np.median)
    st_df['turb_mean'] = st_df.groupby(['Stream WS','year'])['turbidity.ntu'].transform(np.mean)

    st_df['no3_mean'] = st_df.groupby(['Stream WS','year'])['no3.mgL'].transform(np.mean)
    st_df['no3_med'] = st_df.groupby(['Stream WS','year'])['no3.mgL'].transform(np.median)
    st_df['pH_mean'] = st_df.groupby(['Stream WS','year'])['pH'].transform(np.mean)
    st_df['EC_mean'] = st_df.groupby(['Stream WS','year'])['e.coli.cfu'].transform(np.mean)

    st_df = st_df[['Stream WS','year', 'cond_mean', 'cond_med', 'cond_max', 'turb_mean', 'turb_med', 'no3_mean', 'no3_med', 'pH_mean', 'EC_mean']]
    st_df = st_df.drop_duplicates()
    st_df = st_df.sort_values('cond_mean',ascending=False)
    print
    print "Stream Watershed by Year"
    print
    print st_df


#
# provides site breakdown
#
def evaluate_by_site():
    st_df = df.copy()


    st_df['csamples'] = st_df.groupby(['WS','ID'])['conductivity.uscm'].transform('count')
    st_df['tsamples'] = st_df.groupby(['WS','ID'])['turbidity.ntu'].transform('count')
    st_df['nsamples'] = st_df.groupby(['WS','ID'])['no3.mgL'].transform('count')
    st_df['psamples'] = st_df.groupby(['WS','ID'])['pH'].transform('count')
    st_df['esamples'] = st_df.groupby(['WS','ID'])['e.coli.cfu'].transform('count')
    st_df['bsamples'] = st_df.groupby(['WS','ID'])['biological_score'].transform('count')

    #print st_df.head(20)

    st_df['cond_med'] = st_df.groupby(['WS','ID'])['conductivity.uscm'].transform(np.median)
    st_df['cond_mean'] = st_df.groupby(['WS','ID'])['conductivity.uscm'].transform(np.mean)
    st_df['cond_max'] = st_df.groupby(['WS','ID'])['conductivity.uscm'].transform('max')

    st_df['turb_med'] = st_df.groupby(['WS','ID'])['turbidity.ntu'].transform(np.median)
    st_df['turb_mean'] = st_df.groupby(['WS','ID'])['turbidity.ntu'].transform(np.mean)

    st_df['no3_mean'] = st_df.groupby(['WS','ID'])['no3.mgL'].transform(np.mean)
    st_df['pH_mean'] = st_df.groupby(['WS','ID'])['pH'].transform(np.mean)
    st_df['EC_mean'] = st_df.groupby(['WS','ID'])['e.coli.cfu'].transform(np.mean)
    st_df['BS_mean'] = st_df.groupby(['WS','ID'])['biological_score'].transform(np.mean)

    c_df = st_df[['WS', 'ID', 'Stream', 'csamples', 'cond_mean', 'cond_med', 'cond_max', 'turb_mean', 'turb_med','no3_mean','pH_mean', 'EC_mean']]
    c_df = c_df.drop_duplicates()
    c_df = c_df.sort_values('cond_mean',ascending=False)
    print
    print "RANKED BY CONDUCTIVITY"
    print c_df.head(20)

    t_df = st_df[['WS', 'ID', 'Stream', 'tsamples', 'turb_mean', 'turb_med', 'cond_mean', 'cond_med', 'cond_max', 'no3_mean','pH_mean', 'EC_mean']]
    t_df = t_df.drop_duplicates()
    t_df = t_df[t_df['tsamples'] >  2]
    t_df = t_df.sort_values('turb_mean',ascending=False)
    print
    print "RANKED BY TURBIDITY"
    print t_df.head(20)

    n_df = st_df[['WS', 'ID', 'Stream', 'nsamples', 'no3_mean', 'cond_mean', 'cond_med', 'cond_max', 'turb_mean', 'turb_med', 'pH_mean', 'EC_mean']]
    n_df = n_df.drop_duplicates()
    n_df = n_df[n_df['nsamples'] >  2]
    n_df = n_df.sort_values('no3_mean',ascending=False)
    print
    print "RANKED BY NO3"
    print n_df.head(20)

    p_df = st_df[['WS', 'ID', 'Stream', 'psamples', 'pH_mean', 'cond_mean', 'cond_med', 'cond_max', 'turb_mean', 'turb_med','no3_mean', 'EC_mean']]
    p_df = p_df.drop_duplicates()
    p_df = p_df[p_df['psamples'] >  2]
    p_df = p_df.sort_values('pH_mean',ascending=False)
    print
    print "RANKED BY pH"
    print p_df.head(20)

    e_df = st_df[['WS', 'ID', 'Stream', 'esamples', 'EC_mean', 'cond_mean', 'cond_med', 'cond_max', 'turb_mean', 'turb_med','no3_mean','pH_mean']]
    e_df = e_df.drop_duplicates()
    e_df = e_df[e_df['esamples'] >  2]
    e_df = e_df.sort_values('EC_mean',ascending=False)
    print
    print "RANKED BY E.coli"
    print e_df.head(20)

    b_df = st_df[['WS', 'ID', 'Stream', 'bsamples', 'BS_mean', 'cond_mean', 'cond_med', 'cond_max', 'turb_mean', 'turb_med','no3_mean','pH_mean']]
    b_df = b_df.drop_duplicates()
    b_df = b_df[b_df['bsamples'] >  2]
    b_df = b_df.sort_values('BS_mean',ascending=False)
    print
    print "RANKED BY Biological Survey"
    print b_df.head(20)





set_pd_display_options()
df = read_data()
df = clean_data(df)

#evaluate_by_stream("Carr Creek")
#evaluate_by_stream("McNutt Creek")


evaluate_by_site()
evaluate_by_stream_watershed("All")
evaluate_by_month_watershed()
#evaluate_by_month_year()
evaluate_by_watershed()


#evaluate_by_season()
#evaluate_by_year()

evaluate_by_month()

evaluate_by_stream_watershed_year("Tanyard Creek")

evaluate_by_stream_watershed_year("Calls Creek")
#
#evaluate_by_stream_watershed_year("Carr Creek")

#evaluate_by_watershed_year()
#evaluate_site_by_year('NORO', 503)

df = df[df['WSID'] == 'MIDO707']
print
b_df = df[['WS', 'ID', 'Stream', 'month', 'year', 'conductivity.uscm', 'turbidity.ntu', 'no3.mgL', 'pH', 'temperature.c', 'e.coli.cfu', 'Flow']]
b_df = b_df.sort_values('year',ascending=False)
print b_df
#df = df[df['WS'] == 'MIDO']
#df = df[df['ID'] == 707]
