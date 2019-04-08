#!/usr/bin/env python


#
# FREE CLIMATE DATA
# https://www.ncdc.noaa.gov/cdo-web/
#

import pandas as pd
from pandas.tools.plotting import table
import numpy as np

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
from datetime import datetime as dt
import statsmodels.api as sm
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA



def print_full(df):
    pd.set_option('display.max_rows', len(df))
    print(df)
    pd.reset_option('display.max_rows')


def set_pd_display_options():
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)


def read_data():

    #
    # read in UOWN data
    #
    uown_df = pd.read_csv('UOWN_data_datecorrected.csv')

    #
    # read in land use (from Phillip)
    #
    land_use_df = pd.read_csv('UOWN_WS_basins.csv')

    # merge them together into one dataframe
    df = pd.merge(uown_df, land_use_df, on=['ID'], how='left')

    df.to_csv('final.csv')

    print("READ DATA COMPLETE")
    print(df.shape)
    return df



def clean_data(df):
    print
    print("CLEAN DATA")
    print
    # drop entries without conductivity or turbidity
    df = df.dropna(subset=['cond'])
    df = df.dropna(subset=['turb'])
    # some sites are not mapped to watersheds; drop sites not tied to sub-basins
    df = df.dropna(subset=['Watershed'])
    df = df[df['Watershed'] != 'Oconee River']
    df = df[df['Watershed'] != 'North Oconee River']
    df = df[df['Watershed'] != 'Middle Oconee River']

    print
    print("CLEAN DATA DONE")
    return df


def applyFunc(s):
    if s < 11.0:
        return 'Poor'
    elif s > 11.0 and s <= 17.0:
        return 'Fair'
    elif s > 17.0 and s <= 22.0:
        return 'Good'
    else:
        return 'Excellent'


Subbasin = True

set_pd_display_options()
df = read_data()
df = clean_data(df)

df = df.drop_duplicates()
df = df.reset_index()

print(df.head(10))
df['cond_mean'] = df.groupby(['ID'])['cond'].transform(np.mean)
df['bio_mean'] = df.groupby(['Watershed'])['bio'].transform(np.median)
# convert bio_mean to poor (<11), fair (11-16), good (17-22), excellent (> 22)
df['Bio_Rating'] = df['bio_mean'].apply(applyFunc)

df.to_csv("ready_for_pca.csv")
reviewDF = df[['ID', 'Watershed', 'cond_mean', 'DA_km', 'Slope_a', 'OW11', 'DO11', 'DL11', 'DM11', 'DH11', 'BA11', 'DE11', 'EV11', 'MF11', 'SH11', 'GR11', 'PA11', 'CU11', 'WW11', 'HW11']]
reviewDF = reviewDF.drop_duplicates()
reviewDF.to_csv('site_landuse.csv')


# select features of interest for the analysis
features = ['cond', 'turb', 'OW11', 'DO11', 'DL11', 'DM11', 'DH11', 'BA11', 'DE11', 'EV11', 'MF11', 'SH11', 'GR11', 'PA11', 'CU11', 'WW11', 'HW11']

# Separating out the features
x = df.loc[:, features].values

# Separating out the target
y = df.loc[:,['Watershed']].values

# Standardizing the features
x = StandardScaler().fit_transform(x)

# run PCA
pca = PCA(n_components=3, svd_solver='full')
principalComponents = pca.fit_transform(x)

# save off eigenvectors with Features
pca_df = pd.DataFrame(pca.components_)
pca_df.columns = features
pca_df.to_csv('pca_output.csv')

print(pca.explained_variance_ratio_)

plt.plot(np.cumsum(pca.explained_variance_ratio_))
plt.xlabel('number of components')
plt.ylabel('cumulative explained variance')
plt.show()



principalDf = pd.DataFrame(data = principalComponents
             , columns = ['principal component 1', 'principal component 2', 'principal component 3'])

if Subbasin is True:
    finalDf = pd.concat([principalDf, df[['Watershed', 'ID']]], axis=1)
else:
    finalDf = pd.concat([principalDf, df[['Bio_Rating', 'bio_mean', 'Watershed', 'date', 'ID']]], axis=1)
    finalDf = finalDf.dropna(subset=['bio_mean'])

finalDf.to_csv("review.csv")


fig = plt.figure(figsize = (8,8))
ax = fig.add_subplot(1,1,1)
ax.set_xlabel('Principal Component 1', fontsize = 15)
ax.set_ylabel('Principal Component 2', fontsize = 15)
ax.set_title('2 component PCA', fontsize = 20)


if Subbasin is True:
    targets = ["Tanyard Creek", "Brooklyn Creek", "Carr Creek Trib", "Hunnicut Creek", "Cedar Creek", "McNutt Creek", "Calls Creek", "Turkey Creek", "Carr Creek", "Bear Creek", "Shoal Creek", "Barber Creek", "Sandy Creek"]
    colors = ['black', 'brown', 'orange', 'skyblue', 'orange', 'blue', 'green', 'red', 'cyan', 'slategray', 'olive', 'yellow', 'purple']
else:
    targets = ["Poor", "Fair", "Good", "Excellent"]
    colors = ['red', 'yellow', 'lightgreen', 'green']


for target, color in zip(targets,colors):
    if Subbasin is True:
        indicesToKeep = finalDf['Watershed'] == target
    else:
        indicesToKeep = finalDf['Bio_Rating'] == target
    ax.scatter(finalDf.loc[indicesToKeep, 'principal component 1']
               , finalDf.loc[indicesToKeep, 'principal component 2']
               , c = color
               , alpha = 0.5
               , s = 80)
ax.legend(targets)
ax.grid()
plt.title('UOWN Sub-basin Principal Component Analysis')


plt.savefig('pca.png')
plt.show()
