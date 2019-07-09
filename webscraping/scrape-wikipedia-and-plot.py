import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
import astropy.coordinates as coord
from astropy.coordinates import SkyCoord
import astropy.units as units
import matplotlib.pyplot as plt


def get_tables(url):
    '''Scrape the page for all tables. Insert the table data into a Pandas DataFrame.'''
    content = requests.get(url)
    soup = BeautifulSoup(content.text, features='lxml')
    headers = [re.sub(pattern='\[\d\]',
                      repl='',
                      string=i.text.strip()) for i in soup.find_all('table')[0].find_all('th')]

    tables = {}
    for cnt, table in enumerate(soup.find_all('table')):
        key = re.sub(pattern='\[\w+\]',
                     repl='',
                     string=table.findPreviousSibling().text)
        tables[key] = pd.DataFrame(columns=headers)
        temp = []
        inc = 0
        for entry in table.find_all('td'):
            entry = entry.text.strip()
            if len(temp) < len(headers):
                temp.append(entry)
            else:
                tables[key].loc[inc, :] = temp
                temp = [entry]
                inc += 1
        tables[key].loc[inc, :] = temp
    
    return tables

if __name__ == '__main__':
    clusters = get_tables('https://en.wikipedia.org/wiki/List_of_Abell_clusters')
    nebulae = get_tables('https://en.wikipedia.org/wiki/List_of_star-forming_regions_in_the_Local_Group')

    # for each page, aggregate the different tables
    _clusters = pd.DataFrame()
    _nebulae = pd.DataFrame()
    for key in clusters:
        _clusters = pd.concat([_clusters, clusters[key]])
    for key in nebulae:
        _nebulae = pd.concat([_nebulae, nebulae[key]])
    _clusters.rename(columns={'Right ascension (J2000)': 'RA',
                             'Declination (J2000)': 'DEC'}, inplace=True)
    _nebulae.rename(columns={'RA [deg]': 'RA',
                             'Dec [deg]': 'DEC'}, inplace=True)

    classes = {'cluster': _clusters, 'nebula': _nebulae}

    fig = plt.figure(figsize=(16, 14))
    ax = fig.add_subplot(111, projection="mollweide")

    for cls in classes:
        ra = classes[cls]['RA']
        dec = classes[cls]['DEC']
        if cls == 'cluster':
            # \xa0 is the non-breaking space in Latin1 (ISO 8859-1)
            # it is necessary to replace this character
            temp = [SkyCoord(_ra.replace('\xa0', ' '),
                             _dec.replace('\xa0', ' ')) for _ra, _dec in zip(ra, dec)]
        else:
            temp = [SkyCoord(_ra, _dec, unit='deg') for _ra, _dec in zip(ra, dec)]
        ra = coord.Angle([entry.ra for entry in temp])
        ra = ra.wrap_at(180 * units.degree)
        dec = coord.Angle([entry.dec for entry in temp])
        ax.scatter(ra.radian, dec.radian, label=cls)
    ax.legend()
    plt.show()
