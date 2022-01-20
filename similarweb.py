from base import SimilarWeb
from GA_base import GoogleAnalytics

import pandas as pd
import numpy as np

import calendar
import pycountry
import requests
import json
import platform
from dotenv import load_dotenv

import dropbox
from dropbox.files import WriteMode
from dropbox.exceptions import ApiError, AuthError

import sys
import os
from io import BytesIO


params = json.loads(sys.argv[1])

# Param configuration

API_KEY = os.environ['API_KEY']

if params['domains'] == '':
    params['domains'] = DOMAINS = [params['clientDomain'].replace(' ', '').lower()]
else: 
    params['domains'] = DOMAINS = [y for x in [[params['clientDomain'].replace(' ', '').lower()], params['domains'].replace(' ', '').lower().split(',')] for y in x]


START_MONTH = params['start_month']
END_MONTH = params['end_month']
COUNTRY = params['country']
GRANULARITY = "monthly"

if 'app' in params.keys():
    APP = True
else:
    APP = False

# External Functions

def get_col_widths(df):
    idx_max = max([len(str(s)) for s in df.index.values] + [len(str(df.index.name))])
    return [idx_max] +[max([len(str(s)) for s in df[col].values] + [len(col)]) for col in df.columns]
    
def get_n_cols(lst):
    return max([len(i.columns) for i in lst])

def ga_preintegration(df):
    df = df.apply(pd.to_numeric)
    df.columns = df.columns.astype(str)
    return df


try:
    client = SimilarWeb(key=API_KEY)

    GOOGLE_ANALYTICS = False
    if params['viewId']:

        start_date =  pd.Timestamp(START_MONTH).strftime('%Y-%m-01')
        end_date =  pd.Timestamp(END_MONTH)
        end_date = end_date.replace(day = calendar.monthrange(end_date.year, end_date.month)[1]).strftime('%Y-%m-%d')
        DOMAIN = f"GA_{params['clientDomain']}"
        country = pycountry.countries.get(alpha_2="{}".format(COUNTRY))

        ga_api = GoogleAnalytics(view_id=params['viewId'], start_date=start_date, end_date=end_date, domain=DOMAIN, country=country.name)
        params.pop('clientDomain')
        params.pop('viewId')
        GOOGLE_ANALYTICS = True

    # except: params are filled with status and everything so far get returned to user aka writer.closed, writer.saved
    def ftotal_visits():
        df1 = pd.DataFrame()
        for domain in DOMAINS:
            try:
                total_visits = client.total_visits(domain, START_MONTH, END_MONTH, COUNTRY, GRANULARITY)
                df = pd.DataFrame(total_visits["visits"]).rename(columns={"visits": domain})
            except: 
                df = pd.DataFrame({domain: np.NaN}, index=[0])
            df1 = df1.merge(df, on="date") if not df1.empty else df
            
        df1['date'] = df1['date'].apply(lambda x: pd.Timestamp(x).strftime('%Y-%m')) 
        df1 = df1.set_index('date').transpose()
        df1["Total"] = df1.sum(axis=1)

        # Appending GA df
        if GOOGLE_ANALYTICS: 
            try:
                df_ga = ga_api.ga_visits()
                df1 = ga_preintegration(df1)
                df_ga = ga_preintegration(df_ga)
            except: 
                df_ga = pd.DataFrame(columns=df1.columns,index=[DOMAIN])
            
            df1 = df1.append(df_ga)

        df1.index.name = "VISITS"
        df1.to_excel(writer, sheet_name = "visits", startrow=1) 

        worksheet = writer.sheets['visits']

        worksheet.set_column(1, len(df1.columns), None, num_fmt)

        for i, width in enumerate(get_col_widths(df1)):
            worksheet.set_column(i, i, width)

        return df1

    def fvisits_split():
        df1 = pd.DataFrame()
        ds = []
        ms = []
        for domain in DOMAINS: 
            try: 
                visits_split = client.visits_split(domain, START_MONTH, END_MONTH, COUNTRY)
                ds.append(visits_split['desktop_visit_share'])
                ms.append(visits_split['mobile_web_visit_share'])
            except:
                ds.append(np.NaN)
                ms.append(np.NaN) 
        
        df = pd.DataFrame({'domain': DOMAINS, "desktop_share": ds, "mobile_share": ms}).set_index('domain')
        
        df.loc['Mean'] = df.mean()

        if GOOGLE_ANALYTICS: 
            try:
                # Appending GA df
                df_ga = ga_api.ga_devicesplit()

                df = ga_preintegration(df)
                df_ga = ga_preintegration(df_ga)

                
            except Exception as e: 
                df_ga = pd.DataFrame(columns=df.columns,index=[DOMAIN])

            df = df.append(df_ga)

        df.index.name = "DEVICE SHARE"
        df.to_excel(writer, sheet_name = "visits", startrow = df.shape[0] + 5) 

        worksheet = writer.sheets['visits']

        s = df.shape[0] + 6
        e = df.shape[0] + df.shape[0] + 6

        for i in range(s, e):    
            worksheet.set_row(i, None, percent_fmt)

        for i, width in enumerate(get_col_widths(df)):
            worksheet.set_column(i, i, width)

        return df        


    def fdesktop_search_visits_distribution():
        lst = []
        
        for domain in DOMAINS:
        
            sum_b = 0
            n = 0
            total = 0
            try: 
                desktop_search_visits_distribution = client.desktop_search_visits_distribution(domain, START_MONTH, END_MONTH, COUNTRY)
                for i in desktop_search_visits_distribution.get('data', []):
                    total += i['visits_distribution']['branded_visits'] + i['visits_distribution']['non_branded_visits']
                    sum_b += i['visits_distribution']['branded_visits'] / (i['visits_distribution']['branded_visits'] + i['visits_distribution']['non_branded_visits'])
                    n += 1

                branded = sum_b / n
                non_branded = 1 - branded

                lst.append({"domain": domain, "branded": branded, "non_branded":  non_branded,"total_visits": total})
            except: 
                lst.append({"domain": domain, "branded": np.NaN, "non_branded":  np.NaN,"total_visits": np.NaN})
            
        df = pd.DataFrame(lst)

        df = df.set_index('domain')
        df.index.name = "BRANDED & NONBRANDED"
            
        visits_total = df.total_visits.sum()
        branded_total = ((df.branded * df.total_visits).sum()) / visits_total
        nonbranded_total = ((df.non_branded * df.total_visits).sum()) / visits_total
        df.loc['Total'] = [branded_total, nonbranded_total, visits_total]
        
        df.to_excel(writer, sheet_name = "branded_nonbranded") 

        worksheet = writer.sheets['branded_nonbranded']
        worksheet.set_column('B:C', None, percent_fmt)
        worksheet.set_column('D:D', None, num_fmt)

        for i, width in enumerate(get_col_widths(df)):
            worksheet.set_column(i, i, width)

        return df

    # total is total desktop traffic for paid and organic only. 
    # The number deviates from what you find on similarweb - but in fact is more accurate! The monthly values are just not rounded to the nearest decimal figure. 

    def fapi_lite(col):
        lst = []
        
        for domain in DOMAINS:

            try: 
                api_lite = client.api_lite(domain)
                paid_keywords_rolling = api_lite['paid_keywords_rolling_unique_count']
                organic_keywords_rolling = api_lite['organic_keywords_rolling_unique_count']

                paid_keywords_rolling_share = api_lite['paid_keywords_rolling_unique_count'] / (api_lite['paid_keywords_rolling_unique_count'] + api_lite['organic_keywords_rolling_unique_count'])
                organic_keywords_rolling_share = 1 - paid_keywords_rolling_share

                lst.append({"domain": domain, "paid_keywords": paid_keywords_rolling_share, "organic_keywords": organic_keywords_rolling_share})
            except: 
                lst.append({"domain": domain, "paid_keywords": np.NaN, "organic_keywords": np.NaN})

        
        df = pd.DataFrame(lst)

        df = df.set_index('domain')
        df.index.name = "PAID & ORGANIC"
        df = df.assign(Visits=col)

        visits_total = df.Visits.sum()
        paid_total = ((df.paid_keywords * df.Visits).sum()) / visits_total
        organic_total = ((df.organic_keywords * df.Visits).sum()) / visits_total
        df.loc['Total'] = [paid_total, organic_total, visits_total]

        df.to_excel(writer, sheet_name = "paid_organic")  
        
        worksheet = writer.sheets['paid_organic']
        worksheet.set_column('B:C', None, percent_fmt)
        worksheet.set_column('D:D', None, num_fmt)
        
        for i, width in enumerate(get_col_widths(df)):
            worksheet.set_column(i, i, width)

        return df

    # Could also take all kewords and sum up there weight. Not sure if that would give the right result. Computationally more demanding. 

    def foverlap():

        if len(DOMAINS) <= 5:

            try: 
                domains = ",".join(DOMAINS)
                overlap = client.overlap(domains, START_MONTH, END_MONTH, COUNTRY)
            except:
                df = pd.DataFrame(columns=["The API call failed."])
                df.to_excel(writer, sheet_name = "overlap")
                return df

            try:
                comb = []
                comparisons = len(DOMAINS)
                lst = list(overlap['data'].keys())
                for i in lst: comb.append(i.split(',')) 

                n = 1
                for i in range(comparisons): 
                    if n == 1:
                        l = []
                        #first
                        for n_dom in DOMAINS: 
                            dic = {"domain": n_dom}
                            val = overlap['data'][n_dom] * 2
                            for s in overlap['data'].keys():
                                if n_dom in s:
                                    val -= overlap['data'][s]
                                    dic['exclusive_visitor'] = val / overlap['data'][n_dom]

                            l.append(dic)
                        df = pd.DataFrame(l)

                    elif n == comparisons:
                        l = []
                        #last
                        for n_dom in DOMAINS:
                            for i in overlap['data'].keys():
                                if all(c in i for c in DOMAINS):

                                    dic = {"domain": n_dom}
                                    dic["visited_{}_more".format(n-1)] = overlap['data'][i] / overlap['data'][n_dom]

                            l.append(dic)
                        df1 = pd.DataFrame(l)
                        df = df.merge(df1, on="domain")

                    else:
                        l = []
                        for n_dom in DOMAINS:

                            val = 0
                            dic = {"domain": n_dom}
                            for i in [x for x in comb if len(x)==n]: 
                                if n_dom in i: 
                                    val += overlap['data'][",".join(i)]

                            dic["visited_{}_more".format(n-1)] = val / overlap['data'][n_dom]

                            l.append(dic)        
                        df1 = pd.DataFrame(l)
                        df = df.merge(df1, on="domain")
                    n += 1
                    
            except:
                df = pd.DataFrame(columns=["The API call worked, but the retrieved data wasn't structured as exprected."])
                df.to_excel(writer, sheet_name = "overlap")
                return df

            df = df.set_index('domain')
            df.index.name = "OVERLAP"
            df.to_excel(writer, sheet_name = "overlap") 

            worksheet = writer.sheets['overlap']

            worksheet.set_column(1, len(DOMAINS), None, percent_f2_fmt)

            for i, width in enumerate(get_col_widths(df)):
                worksheet.set_column(i, i, width)

            return df 

        df = pd.DataFrame(columns=["To many domains were passed. This function allows for maximum five domains (including the clients."])
        df.to_excel(writer, sheet_name = "overlap")
        return df

    def fdesktop_overview_share():
        df1 = pd.DataFrame()
        for domain in DOMAINS: 
            try:
                desktop_overview_share = client.desktop_overview_share(domain, START_MONTH, END_MONTH, COUNTRY)
                df = pd.DataFrame(data=desktop_overview_share.get("overview", [])).groupby("source_type").sum()
                df = df.rename(columns={"share" : domain}).transpose().reset_index().rename_axis(None,axis=1)
            except: 
                pd.DataFrame({"index": [domain]})
            df1 = df1.append(df) if not df1.empty else df

        if GOOGLE_ANALYTICS: 
            try:
                # Appending GA df
                df1.columns = df1.columns.astype(str)
                df1 = df1.rename(columns={"index": "DESKTOP"})
                df_x, df_ga = ga_api.ga_channels()
                df_ga.columns = df_ga.columns.astype(str)
            except: 
                df_ga = pd.DataFrame(columns=df1.columns,index=[DOMAIN])
            
            df1 = df1.append(df_ga)
            df1 = df1.set_index('DESKTOP')

        else: 
            df1 = df1.set_index('index')
            df1.index.name = "DESKTOP"
        df1 = df1.reindex(sorted(df1.columns), axis=1)
        return df1

    def fmobile_overview_share():
        df1 = pd.DataFrame()
        for domain in DOMAINS:
            try:
                mobile_overview_share = client.mobile_overview_share(domain, START_MONTH, END_MONTH, COUNTRY)
                lst = []
                total = 0
                for i in mobile_overview_share['visits'][domain]:
                    dic = {}
                    dic['channel'] = i['source_type']

                    sv = 0
                    for v in i['visits']:
                        sv += v['visits']

                    total += sv
                    dic['visits'] = sv
                    lst.append(dic)

                for dic in lst:
                    dic['visits'] = dic['visits'] / total

                df = pd.DataFrame(lst)
                df = df.rename(columns={"visits" : domain}).transpose().reset_index().rename_axis(None,axis=1)
                new_header = df.iloc[0]
                df = df[1:]
                df.columns = new_header
            except: 
                df = pd.DataFrame({"channel": [domain]}, index=[0])

            df1 = df1.append(df) if not df1.empty else df
        if GOOGLE_ANALYTICS: 
            try:
                # Appending GA df
                df1.columns = df1.columns.astype(str)
                df1 = df1.rename(columns={"channel": "MOBILE"})
                df_ga, df_x = ga_api.ga_channels()
                df_ga.columns = df_ga.columns.astype(str)
            except: 
                df_ga = pd.DataFrame(columns=df1.columns,index=[DOMAIN])
            df1 = df1.append(df_ga) 
            df1 = df1.set_index('MOBILE')
        else: 
            df1 = df1.set_index('channel')
            df1.index.name = "MOBILE"
        df1 = df1.reindex(sorted(df1.columns), axis=1)  
        return df1


    def fchannel_overview_share(dfs, stotal):

        try: 
            dfd = fdesktop_overview_share()
            dfm = fmobile_overview_share()
            
            # df select the rows with index that is in DOMAINS + append stotal + desktop and mobile *stotal

            dfs['Total'] = stotal
            df = dfs.loc[DOMAINS, :]
            df.desktop_share = df.desktop_share * df.Total
            df.mobile_share = df.mobile_share * df.Total

            # format excel

            result_dfd =  all(elem in dfd.columns for elem in ['Search / Organic', 'Search / Paid'])
            result_dfm =  all(elem in dfm.columns for elem in ['Search'])
            if result_dfd and result_dfm:
                dfm['Organic Search'] = (dfd['Search / Organic'] / (dfd['Search / Organic'] + dfd['Search / Paid'])) * dfm['Search'] # somewhere here
                dfm['Paid Search'] = (dfd['Search / Paid'] / (dfd['Search / Organic'] + dfd['Search / Paid'])) * dfm['Search'] # somewhere here
            
            df_sum = pd.DataFrame(columns = ['Direct', 'Social', 'Organic', 'Paid Media', 'Others'], index = DOMAINS)

            result_dfd =  all(elem in dfd.columns for elem in ['Direct'])
            result_dfm =  all(elem in dfm.columns for elem in ['Direct'])
            if result_dfd and result_dfm:
                df_sum.Direct = ((dfd.Direct * df.desktop_share)+(dfm.Direct * df.mobile_share)) / df.Total


            result_dfd =  all(elem in dfd.columns for elem in ['Social'])
            result_dfm =  all(elem in dfm.columns for elem in ['Social'])
            if result_dfd and result_dfm:            
                df_sum.Social = ((dfd.Social * df.desktop_share)+(dfm.Social * df.mobile_share)) / df.Total
            
            result_dfd =  all(elem in dfd.columns for elem in ['Search / Organic'])
            result_dfm =  all(elem in dfm.columns for elem in ['Organic Search'])
            if result_dfd and result_dfm: 
                df_sum.Organic = ((dfd['Search / Organic'] * df.desktop_share)+(dfm['Organic Search'] * df.mobile_share)) / df.Total
                df_sum['Paid Media'] = ((dfd['Search / Paid'] * df.desktop_share)+(dfm['Paid Search'] * df.mobile_share)) / df.Total

            result_dfd =  all(elem in dfd.columns for elem in ['Email', 'Other', 'Referral'])
            result_dfm =  all(elem in dfm.columns for elem in ['Mail', 'Referrals'])
            if result_dfd and result_dfm: 
                df_sum.Others = ((((dfd.Email + dfd.Other + dfd.Referral) * df.desktop_share))+((dfm.Mail + dfm.Referrals) * df.mobile_share)) / df.Total
            max_col = get_n_cols([dfd, dfm, df, df_sum]) + 1
            
            dfd.to_excel(writer, sheet_name = "channel")   
            dfm.to_excel(writer, sheet_name = "channel", startrow=dfd.shape[0]+3) 
            df.to_excel(writer, sheet_name = "channel", startcol=max_col+3)
            df_sum.to_excel(writer, sheet_name = "channel", startcol=1, startrow=(dfd.shape[0]+dfm.shape[0]+6))

            worksheet = writer.sheets['channel']
            
            worksheet.set_column(1, max_col, None, percent_f2_fmt)
            worksheet.set_column(max_col+4, max_col+4+len(df.columns), None, num_fmt)

            for x, n in enumerate([dfd, dfm, df, df_sum]): 
                for i, width in enumerate(get_col_widths(n)):
                    if x == 2:
                        i += 1
                    if x == 3:
                        i += max_col + 2
                    worksheet.set_column(i, i, width) 
        except: 
            workbook  = writer.book
            worksheet = workbook.add_worksheet('channel') 



    def fgender():
        lst = []
        
        for domain in DOMAINS:
            try: 
                gender = client.gender(domain, START_MONTH, END_MONTH, COUNTRY)
                lst.append({"domain": domain, "male": gender['male'], "female": gender['female']})
            except:
                lst.append({"domain": domain, "male": np.NaN, "female": np.NaN})
        
        df = pd.DataFrame(lst)

        
        if GOOGLE_ANALYTICS: 

            try:
                df.columns = df.columns.astype(str)
                dfg_ga = ga_api.ga_gender()
                dfg_ga.columns = dfg_ga.columns.astype(str)
            except: 
                dfg_ga = pd.DataFrame(columns=df.columns,index=[DOMAIN])
            df = df.append(dfg_ga)

        df = df.set_index('domain')
        df.index.name = "GENDER"
        return df

    def fage():
        lst = []
        
        for domain in DOMAINS:
            try: 
                age = client.age(domain, START_MONTH, END_MONTH, COUNTRY)
                lst.append({"domain": domain, "18-24": age['age_18_to_24'], "25-34": age['age_25_to_34'], 
                            "35-44": age['age_35_to_44'], "45-54": age['age_45_to_54'], "55-64": age['age_55_to_64'], 
                            "65+": age['age_65_plus']})
            except:
                lst.append({"domain": domain, "18-24": np.NaN, "25-34": np.NaN, 
                            "35-44": np.NaN, "45-54": np.NaN, "55-64": np.NaN, 
                            "65+": np.NaN})
        
        df = pd.DataFrame(lst)

        if GOOGLE_ANALYTICS:
            try:
                df.columns = df.columns.astype(str)
                dfa_ga = ga_api.ga_age()
                dfa_ga.columns = dfa_ga.columns.astype(str)
            except: 
                dfa_ga = pd.DataFrame(columns=df.columns,index=[DOMAIN])
            df = df.append(dfa_ga)

        df = df.set_index('domain')
        df.index.name = "AGE"
        return df

    def faudience(stotal):
        dfg = fgender()
        dfa = fage()

        for df in [dfg, dfa]:
            df['Visits'] = stotal
            total_visits = df['Visits'].loc[DOMAINS].sum()
            
            lst = []
            for col in df.columns: 
                if col == "Visits":
                    lst.append(total_visits)
                else:
                    lst.append(((df[col].loc[DOMAINS] * df.Visits).loc[DOMAINS].sum()) / total_visits)
            df.loc["Total"] = lst


        dfg.to_excel(writer, sheet_name = "audience")
        dfa.to_excel(writer, sheet_name = "audience", startcol=dfg.shape[1]+2)

        worksheet = writer.sheets['audience']

        worksheet.set_column("B:C", None, percent_fmt)
        worksheet.set_column("D:D", None, num_fmt)
        worksheet.set_column("F:L", None, percent_fmt)
        worksheet.set_column("M:M", None, num_fmt)

        for i, width in enumerate(get_col_widths(dfg)):
            worksheet.set_column(i, i, width)
        for i, width in enumerate(get_col_widths(dfa)):
            i += dfg.shape[1]+2
            worksheet.set_column(i, i, width)

    def fsocial():
        lst = []
        
        for domain in DOMAINS:
            try:
                social = client.social(domain, START_MONTH, END_MONTH, COUNTRY)
                dic = {'domain': domain, 'youtube': 0, 'linkedin': 0, 'reddit': 0, 'twitter': 0, 'facebook': 0, 'pinterest': 0, 'instagram': 0, 'whatsapp': 0}
                for page in social['social']:
                    for i in ['youtube', 'linkedin', 'reddit', 'twitter', 'facebook', 'pinterest', 'instagram', 'whatsapp']:
                        if i in page['page'].lower(): 
                            dic[i] += page["share"]
            except: 
                dic = {'domain': domain, 'youtube': np.NaN, 'linkedin': np.NaN, 'reddit': np.NaN, 'twitter': np.NaN, 'facebook': np.NaN, 'pinterest': np.NaN, 'instagram': np.NaN, 'whatsapp': np.NaN}

            lst.append(dic)

        df = pd.DataFrame(lst)

        if GOOGLE_ANALYTICS: 
            try:
                # Appending GA df
                df.columns = df.columns.astype(str)
                df_ga = ga_api.ga_social()
                df_ga.columns = df_ga.columns.astype(str)  
            except:
                df_ga = pd.DataFrame(columns=df.columns,index=[DOMAIN])

            df = df.append(df_ga)    
            df = df.fillna(0)

        df = df.set_index('domain')
        df.index.name = "SOCIAL"
        df.to_excel(writer, sheet_name = "social") 

        worksheet = writer.sheets['social']

        worksheet.set_column("B:I", None, percent_fmt)

        for i, width in enumerate(get_col_widths(df)):
            worksheet.set_column(i, i, width)
        
        return df

    def fgeo():
        try:
            traffic_by_country = client.traffic_by_country(DOMAINS[0], START_MONTH, END_MONTH)
            df = pd.DataFrame(traffic_by_country['records'])[["country_name", "share"]].sort_values(by=['share']).tail(10).rename(columns={"share": DOMAINS[0]}).transpose()
            new_header = df.iloc[0]
            df = df[1:]
            df.columns = new_header 

            if len(DOMAINS) > 1: 

                for domain in DOMAINS[1:]:
                    try:
                        traffic_by_country = client.traffic_by_country(domain, START_MONTH, END_MONTH)
                        min_data = pd.DataFrame(traffic_by_country['records'])[["country_name", "share"]].rename(columns={"share": domain})

                        bs=[]    
                        b=(min_data["country_name"].isin([i for i in df.columns]))
                        bs.append(b)
                        matches=pd.concat(bs,1)
                        min_data = min_data[(matches == True).any(1)].transpose()

                        new_header = min_data.iloc[0]
                        min_data = min_data[1:]
                        min_data.columns = new_header 
                    except: 
                        min_data.loc[domain] = {}

                    df = pd.concat([df, min_data], axis=0)

            if GOOGLE_ANALYTICS: 
                # Appending GA df
                try: 
                    df.columns = df.columns.astype(str)
                    df_ga = ga_api.ga_geo(df.columns)
                    df_ga.columns = df_ga.columns.astype(str)
                except: 
                    df_ga = pd.DataFrame(columns=df.columns,index=[DOMAIN])
                df = df.append(df_ga)
                 
        except: 
            df = pd.DataFrame()

        df = df[df.columns[::-1]]

        df.index.name = "GEO"
        df.to_excel(writer, sheet_name = "geo") 

        worksheet = writer.sheets['geo']

        worksheet.set_column("B:K", None, percent_fmt)

        for i, width in enumerate(get_col_widths(df)):
            worksheet.set_column(i, i, width)
        
        return df

    def fapp_dau(domain, app_domains):
        df1 = pd.DataFrame()
        
        for app_domain in app_domains: 
            try:
                app_dau = client.app_dau(app_domain, START_MONTH, END_MONTH, COUNTRY, GRANULARITY) 
                df = pd.DataFrame(app_dau['daily_active_users'])[['start_date', 'active_users']].rename(columns={'start_date': 'DAU', 'active_users': app_domain})
            except: 
                df = pd.DataFrame(columns=['DAU', app_domain])

            df1 = df1.merge(df, on="DAU") if not df1.empty else df
        
        df1['DAU'] = df1['DAU'].apply(lambda x: pd.Timestamp(x).strftime('%Y-%m'))    
        df1 = df1.set_index('DAU').transpose()
        df1["DAU"] = domain
        df1 = df1.groupby(by=["DAU"]).sum().reset_index()
            
        return df1.set_index('DAU')

    def fapp_mau(domain, app_domains):
        df1 = pd.DataFrame()
        for app_domain in app_domains: 
            try:
                app_mau = client.app_mau(app_domain, START_MONTH, END_MONTH, COUNTRY, GRANULARITY)   

                df = pd.DataFrame(app_mau['monthly_active_users'])[['start_date', 'active_users']].rename(columns={'start_date': 'MAU', 'active_users': app_domain})
            except: 
                df = pd.DataFrame(columns=['MAU', app_domain])

            df1 = df1.merge(df, on="MAU") if not df1.empty else df
        
        df1['MAU'] = df1['MAU'].apply(lambda x: pd.Timestamp(x).strftime('%Y-%m'))    
        df1 = df1.set_index('MAU').transpose()
        df1["MAU"] = domain
        df1 = df1.groupby(by=["MAU"]).sum().reset_index()

        return df1.set_index('MAU')

    def fapp_downloads(domain, app_domains):
        df1 = pd.DataFrame()
        
        for app_domain in app_domains: 
            try:
                app_downloads = client.app_downloads(app_domain, START_MONTH, END_MONTH, COUNTRY, GRANULARITY) 
                df = pd.DataFrame(app_downloads['downloads'])[['start_date', 'downloads']].rename(columns={'start_date': 'Downloads', 'downloads': app_domain})
        
            except: 
                df = pd.DataFrame(columns=['Downloads', app_domain])

            df1 = df1.merge(df, on="Downloads") if not df1.empty else df
        
        df1['Downloads'] = df1['Downloads'].apply(lambda x: pd.Timestamp(x).strftime('%Y-%m'))    
        df1 = df1.set_index('Downloads').transpose()
        df1["Downloads"] = domain
        df1 = df1.groupby(by=["Downloads"]).sum().reset_index()
            
        return df1.set_index('Downloads')

    def fapp_gender(domain, app_domains):
        lst = []
        
        for app_domain in app_domains: 
            try:   
                app_gender = client.app_gender(app_domain, COUNTRY)  
                lst.append({"Gender": app_domain, "male": app_gender['male'], "female": app_gender['female']})
            except:
                lst.append({"Gender": app_domain, "male": np.NaN, "female": np.NaN})

        df = pd.DataFrame(lst).groupby(by="Gender").mean().reset_index()
        return df.set_index('Gender')

    def fapp_age(domain, app_domains):
        lst = []
        
        for app_domain in app_domains:   
            try:
                app_age = client.app_age(app_domain, COUNTRY)  
                lst.append({"Age": app_domain, "18-24": app_age['age_18_to_24'], "25-34": app_age['age_25_to_34'], 
                            "35-44": app_age['age_35_to_44'], "45-54": app_age['age_45_to_54'], "55+": app_age['age_55_plus']})
            except:
                lst.append({"Age": app_domain, "18-24": np.Nan, "25-34": np.Nan, "35-44": np.Nan, "45-54": np.Nan, "55+": np.Nan})
        
        df = pd.DataFrame(lst).groupby(by="Age").mean().reset_index()
        return df.set_index('Age')

    def frelated_apps():
        df_downloads = pd.DataFrame()
        df_dau = pd.DataFrame()
        df_mau = pd.DataFrame()
        df_gender = pd.DataFrame()
        df_age = pd.DataFrame()
        
        for domain in domains:
            try:  
                related_apps = client.related_apps(domain)
                app_domains = [i['app_id'] for i in related_apps['related_apps']]
            except: 
                app_domains = []
            
            df_dau = pd.concat([df_dau, fapp_dau(domain, app_domains)], axis=0) if not df_dau.empty else fapp_dau(domain, app_domains)
            df_mau = pd.concat([df_mau, fapp_mau(domain, app_domains)], axis=0) if not df_mau.empty else fapp_mau(domain, app_domains)
            df_downloads = pd.concat([df_downloads, fapp_downloads(domain, app_domains)], axis=0) if not df_downloads.empty else fapp_downloads(domain, app_domains)
            df_gender = pd.concat([df_gender, fapp_gender(domain, app_domains)], axis=0) if not df_gender.empty else fapp_gender(domain, app_domains)
            df_age = pd.concat([df_age, fapp_age(domain, app_domains)], axis=0) if not df_age.empty else fapp_age(domain, app_domains)
        

        df_dau.to_excel(writer, sheet_name = "App", startrow=1, startcol=0) 
        df_mau.to_excel(writer,sheet_name='App',startrow=(df_dau.shape[0]+5), startcol=0)
        df_downloads.to_excel(writer,sheet_name='App',startrow=(df_dau.shape[0]+df_mau.shape[0]+7), startcol=0)
        df_gender.to_excel(writer,sheet_name='App',startrow=(df_dau.shape[0]+df_mau.shape[0]+df_downloads.shape[0]+9), startcol=0)
        df_age.to_excel(writer,sheet_name='App',startrow=(df_dau.shape[0]+df_mau.shape[0]+df_downloads.shape[0]+df_gender.shape[0]+11), startcol=0)
        
        

    # Execute
    def execute():
        dfv = ftotal_visits()                          # fully working for GA
        dfs = fvisits_split()                          # fully working for GA
        dfb = fdesktop_search_visits_distribution()    
        fapi_lite(dfb['total_visits'])
        foverlap() 
        fchannel_overview_share(dfs, dfv['Total'])     # fully working for GA
        faudience(dfv['Total'])                        # fully working for GA
        fsocial()                                      # fully working for GA (strange numbers)
        fgeo()                                         # fully working for GA       

        # still havent had an opportunity to test app             
        if APP == True:
            frelated_apps()


    TOKEN = os.environ['TOKEN']
    BACKUPPATH = f"/{params['file']}.xlsx"
    params['path'] = BACKUPPATH

    dbx = dropbox.Dropbox(TOKEN)  
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')

    workbook  = writer.book

    percent_fmt = workbook.add_format({'num_format': '0%'}) 
    percent_f2_fmt = workbook.add_format({'num_format': '0.00%'})
    num_fmt = workbook.add_format({'num_format': '#,###'})
    
    execute()

    writer.save()
    output.seek(0)

    dbx.files_upload(output.getvalue(), path=BACKUPPATH, mode=dropbox.files.WriteMode.overwrite)

    params['status'] = 'success'

    
except Exception as e:
    print(e, end='')


print(params)


""" ToDo """
# add analytics branded/nonbranded and paid/organic
# add checkboxes for functions
# prevent everything from re-running in case of minor issue. Reuse worksheet and track progress 
# figure out error handling