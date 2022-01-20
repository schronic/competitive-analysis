""" Requires me to login. We should find a way around this?! How should authentication go? Maybe create a ps overarching entitity """

import argparse

from apiclient.discovery import build
import httplib2
from oauth2client import client as cli
from oauth2client import file
from oauth2client import tools

import numpy as np
import pandas as pd

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
CLIENT_SECRETS_PATH = 'ga/client_secrets.json' # Path to client_secrets.json file.

class GoogleAnalytics(object):

    def initialize_analyticsreporting(self):

        # Parse command-line arguments.
        parser = argparse.ArgumentParser(
            formatter_class=argparse.RawDescriptionHelpFormatter,
            parents=[tools.argparser])
        flags = parser.parse_args([])

        # Set up a Flow object to be used if we need to authenticate.
        flow = cli.flow_from_clientsecrets(
            self.client_secrets_path, scope=self.scopes,
            message=tools.message_if_missing(self.client_secrets_path))

        # Prepare credentials, and authorize HTTP object with them.
        # If the credentials don't exist or are invalid run through the native client
        # flow. The Storage object will ensure that if successful the good
        # credentials will get written back to a file.
        storage = file.Storage('analyticsreporting.dat')
        credentials = storage.get()
        if credentials is None or credentials.invalid:
            credentials = tools.run_flow(flow, storage, flags)
        http = credentials.authorize(http=httplib2.Http())

        # Build the service object.
        analytics = build('analyticsreporting', 'v4', http=http)

        return analytics
    
    def __init__(self, view_id, start_date, end_date, domain, country):
        if not view_id:
            raise Exception('the view ID needs to be defined')

        self.scopes = SCOPES
        self.client_secrets_path = CLIENT_SECRETS_PATH # Path to client_secrets.json file.
        self.view_id = view_id 
        self.start_date = start_date
        self.end_date = end_date 
        self.domain = domain 
        self.ga_country = country 

        analytics = self.initialize_analyticsreporting()
        self.analytics = analytics


    def ga_response_dataframe(self, response, dims = [], metrics = []):
        data_dic = {f"{i}": [] for i in dims + metrics}
        for report in response.get('reports', []):
            rows = report.get('data', {}).get('rows', [])
            for row in rows:
                for i, key in enumerate(dims):
                    data_dic[key].append(row.get('dimensions', [])[i]) # Get dimensions
                dateRangeValues = row.get('metrics', [])
                for values in dateRangeValues:
                    all_values = values.get('values', []) # Get metric values
                    for i, key in enumerate(metrics):
                        data_dic[key].append(all_values[i])

        df = pd.DataFrame(data=data_dic)
        df.columns = [col.split(':')[-1] for col in df.columns]
        return df

    def get_report(self, dims = [], metrics = []):
    # Use the Analytics Service Object to query the Analytics Reporting API V4.
        requests_list =  [{
            'viewId': self.view_id, 
            'dateRanges': [{'startDate': self.start_date, 'endDate': self.end_date}],
            'dimensions': [{'name': name} for name in dims],
            'metrics': [{'expression': exp} for exp in metrics],
            "samplingLevel": "LARGE",
            "pageSize": 10000
            }]
        return self.analytics.reports().batchGet(body={'reportRequests':requests_list }).execute()

   


    # ### Visits
    def ga_visits(self):
        dims = ['ga:date', 'ga:country', 'ga:userType']
        metrics = ['ga:users']
        response = self.get_report(dims, metrics)

        df = self.ga_response_dataframe(response, dims, metrics)
        df = df[df['country'] == self.ga_country].drop(['country'], axis=1)
        df = df[df['userType'] == "New Visitor"].drop(['userType'], axis=1)
        df['date'] = pd.to_datetime(df.date)
        df['users'] = pd.to_numeric(df.users) 
        
        g = df.groupby(df.date.dt.to_period("M")).sum()
        g.loc['Total'] = g.sum()
        g = g.rename(columns={"users": self.domain}).transpose().rename_axis(None,axis=1)
        return g


    # ### Desktop/ Mobile Split
    def ga_devicesplit(self):
        dims = ['ga:country', 'ga:deviceCategory']
        metrics = ['ga:sessions']

        response = self.get_report(dims, metrics)
        df = self.ga_response_dataframe(response, dims, metrics)

        df = df[df['country'] == self.ga_country].drop(['country'], axis=1)
        df['sessions'] = pd.to_numeric(df.sessions) 
        g = df.groupby(['deviceCategory']).sum()
        g.loc['mobile'] += g.loc['tablet']
        g = g.drop(columns='tablet')
        g.loc['desktop'] = g.loc['desktop'] / g.sum()
        g.loc['mobile'] = g.loc['mobile'] / g.sum()
        g = g.rename(columns={"sessions": self.domain}).transpose().rename_axis(None,axis=1)
        g = g.add_suffix('_share')
        return g

    def ga_channels(self):
        dims = ['ga:country', 'ga:deviceCategory', 'ga:acquisitionTrafficChannel'] #, 'ga:country', 'ga:source', 'ga:sourceMedium', 'ga:medium', 'ga:deviceCategory'
        metrics = ['ga:sessions']

        response = self.get_report(dims, metrics)
        df = self.ga_response_dataframe(response, dims, metrics)
        df = df[df['country'] == self.ga_country].drop(['country'], axis=1)
        df['sessions'] = pd.to_numeric(df.sessions) 
        df = df.rename(columns={"sessions": self.domain})

        df_mobile = df[df.deviceCategory == 'mobile'].drop(['deviceCategory'], axis=1).set_index('acquisitionTrafficChannel').transpose().rename_axis(None, axis=1)
        df_desktop = df[df.deviceCategory == 'desktop'].drop(['deviceCategory'], axis=1).set_index('acquisitionTrafficChannel').transpose().rename_axis(None, axis=1)
        df_mobile = df_mobile.div(df_mobile.sum(axis=1), axis=0)
        df_desktop = df_desktop.div(df_desktop.sum(axis=1), axis=0)

        df_mobile['Search'] = df_mobile['Organic Search'] + df_mobile['Paid Search']
        df_mobile = df_mobile.reset_index().rename(columns={"index": "MOBILE", "Email": "Mail", "Referral": "Referrals", "(Other)": "Other", "Display": "Display Ads"}).drop(columns=['Organic Search', 'Paid Search'])
        df_desktop = df_desktop.reset_index().rename(columns={"index": "DESKTOP", "(Other)": "Other", "Display": "Display Ad", "Organic Search": "Search / Organic", "Paid Search": "Search / Paid"})
        return df_mobile, df_desktop


    # ### audience
    def ga_gender(self):
        dims = ['ga:country', 'ga:userGender'] 
        metrics = ['ga:sessions']

        response = self.get_report(dims, metrics)
        df = self.ga_response_dataframe(response, dims, metrics)
        df = df[df['country'] == self.ga_country].drop(['country'], axis=1)
        df['sessions'] = pd.to_numeric(df.sessions) 
        df = df.rename(columns={"sessions": self.domain})

        df = df.set_index('userGender').transpose().rename_axis(None, axis=1)
        df = df.div(df.sum(axis=1), axis=0)
        df = df.reset_index().rename(columns={'index': 'domain'})
        return df


    def ga_age(self):
        dims = ['ga:country', 'ga:userAgeBracket']
        metrics = ['ga:sessions']

        response = self.get_report(dims, metrics)
        df = self.ga_response_dataframe(response, dims, metrics)
        df = df[df['country'] == self.ga_country].drop(['country'], axis=1)
        df['sessions'] = pd.to_numeric(df.sessions) 
        df = df.rename(columns={"sessions": self.domain})

        df = df.set_index('userAgeBracket').transpose().rename_axis(None, axis=1)
        df = df.div(df.sum(axis=1), axis=0)
        df = df.reset_index().rename(columns={'index': 'domain'})
        return df


    # ### social
    def ga_social(self):  
        dims = ['ga:country', 'ga:socialNetwork']
        metrics = ['ga:sessions']

        response = self.get_report(dims, metrics)
        df = self.ga_response_dataframe(response, dims, metrics)
        df = df[df['country'] == self.ga_country].drop(['country'], axis=1)
        df['sessions'] = pd.to_numeric(df.sessions) 
        df = df.rename(columns={"sessions": self.domain})

        df = df.set_index('socialNetwork').transpose().rename_axis(None, axis=1)
        df = df.div(df.sum(axis=1), axis=0)
        df.columns = map(str.lower, df.columns)
        df = df.reset_index().rename(columns={'index': 'domain'})

        df = df[df.columns[df.columns.isin(['domain', 'youtube', 'linkedin', 'reddit', 'twitter', 'facebook', 'pinterest', 'instagram', 'whatsapp'])]]
        return df

    # ### Geo
    def ga_geo(self, cols): 
        # The number closer to what similarweb displays is all traffic; but similarweb atleast on paper limits themselves to desktop
        dims = ['ga:country']
        metrics = ['ga:users']
        response = self.get_report(dims, metrics)
        df = self.ga_response_dataframe(response, dims, metrics)
        df['users'] = pd.to_numeric(df.users) 
        
        df['users'] = df['users'].div(df['users'].sum())
        df = df[df["country"].isin([i for i in cols])].set_index('country').rename(columns={'users': self.domain}).transpose().rename_axis(None,axis=1)

        return df

