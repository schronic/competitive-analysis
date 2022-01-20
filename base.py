
import requests
import json

SIMILARWEB_API_URL = 'https://api.similarweb.com/v1/'

class SimilarWeb(object):

    def __init__(self, key):
        if not key:
            raise Exception('A Similarweb key must be provided')

        self.api_url = SIMILARWEB_API_URL
        self.key = key
        
    # Report producing methods
    def produce(self, report_type, endpoint, **kwargs):
        # report_type = total-traffic-and-engagement
        # endpoint = bounce-rate
        
        data = self.retrieve(report_type, endpoint, **kwargs)
        return json.loads(data.text)

    def retrieve(self, report_type, endpoint, **kwargs):

        if 'app' in kwargs: 
            mid_url = "app/Google/{0}/{1}/{2}?".format(kwargs["domain"], report_type, endpoint)
        elif 'overlap' in kwargs:
            mid_url = "website/{0}/{1}?".format(report_type, endpoint) 
        else: 
            mid_url = "website/{0}/{1}/{2}?".format(kwargs["domain"], report_type, endpoint)
        
        url_base = self.api_url + mid_url
        
        kwargs['api_key'] = self.key
        
        payload = kwargs.copy()
        try: 
            payload.pop("domain")
        except: 
            pass

        response = requests.get(url_base, params=payload)

        if response.status_code == 200:
            return response
        else:
            raise Exception(response.content)

    # Utilitites
    def capabilities(self, **kwargs):
        kwargs['api_key'] = self.key
        url_base = 'https://api.similarweb.com/capabilities?'
        response = requests.get(url_base, params=kwargs)
        return json.loads(response.text)
             
    # Total Traffic
    def total_visits(self, domain, start_date, end_date, country, granularity, **kwargs):

        return self.produce('total-traffic-and-engagement', 'visits', domain=domain, start_date=start_date, end_date=end_date, country=country, granularity=granularity, **kwargs)
    

    def pages_per_visit(self, domain, start_date, end_date, country, granularity, **kwargs):
                              
        return self.produce('total-traffic-and-engagement', 'pages-per-visit', domain=domain, start_date=start_date, end_date=end_date, country=country, granularity=granularity, **kwargs)

    
    def average_visit_duration(self, domain, start_date, end_date, country, granularity, **kwargs):

        return self.produce('total-traffic-and-engagement', 'average-visit-duration', domain=domain, start_date=start_date, end_date=end_date, country=country, granularity=granularity, **kwargs)


    def bounce_rate(self, domain, start_date, end_date, country, granularity, **kwargs):

        return self.produce('total-traffic-and-engagement', 'bounce-rate', domain=domain, start_date=start_date, end_date=end_date, country=country, granularity=granularity, **kwargs)


    def visits_split(self, domain, start_date, end_date, country, **kwargs):
        
        return self.produce('total-traffic-and-engagement', 'visits-split', domain=domain, start_date=start_date, end_date=end_date, country=country, **kwargs)

    def deduplicated_audiences(self, domain, start_date, end_date, country, **kwargs):
        
        return self.produce('dedup', 'deduplicated-audiences', domain=domain, start_date=start_date, end_date=end_date, country=country, **kwargs)

    # Desktop Traffic
    def desktop_visits(self, domain, start_date, end_date, country, granularity, **kwargs):
        
        return self.produce('traffic-and-engagement', 'visits', domain=domain, start_date=start_date, end_date=end_date, country=country, granularity=granularity, **kwargs)

    def desktop_pages_per_visit(self, domain, start_date, end_date, country, granularity, **kwargs):
        
        return self.produce('traffic-and-engagement', 'pages-per-visit', domain=domain, start_date=start_date, end_date=end_date, country=country, granularity=granularity, **kwargs)
   
    def desktop_average_visit_duration(self, domain, start_date, end_date, country, granularity, **kwargs):
        
        return self.produce('traffic-and-engagement', 'average-visit-duration', domain=domain, start_date=start_date, end_date=end_date, country=country, granularity=granularity, **kwargs)
  
    def desktop_bounce_rate(self, domain, start_date, end_date, country, granularity, **kwargs):
        
        return self.produce('traffic-and-engagement', 'bounce-rate', domain=domain, start_date=start_date, end_date=end_date, country=country, granularity=granularity, **kwargs)
   
    def desktop_global_rank(self, domain, start_date, end_date, **kwargs):
        
        return self.produce('global-rank', 'global-rank', domain=domain, start_date=start_date, end_date=end_date, **kwargs)
   
    def desktop_country_rank(self, domain, start_date, end_date, country, **kwargs):
        
        return self.produce('country-rank', 'country-rank', domain=domain, start_date=start_date, end_date=end_date, country=country, **kwargs)

    def desktop_traffic_by_country(self, domain, start_date, end_date, **kwargs):
        
        return self.produce('geo', 'traffic-by-country', domain=domain, start_date=start_date, end_date=end_date,**kwargs)
    
    def desktop_unique_visitors(self, domain, start_date, end_date, country, granularity, **kwargs):
        
        return self.produce('unique-visitors', 'desktop_unique_visitors', domain=domain, start_date=start_date, end_date=end_date, country=country, granularity=granularity, **kwargs)
    
    # Mobile Web Traffic
    def mobile_visits(self, domain, start_date, end_date, country, granularity, **kwargs):
        
        return self.produce('mobile-web', 'visits', domain=domain, start_date=start_date, end_date=end_date, country=country, granularity=granularity, **kwargs)
    
    def mobile_pages_per_visit(self, domain, start_date, end_date, country, granularity, **kwargs):
        
        return self.produce('mobile-web', 'pages-per-visit', domain=domain, start_date=start_date, end_date=end_date, country=country, granularity=granularity, **kwargs)
   
    def mobile_average_visit_duration(self, domain, start_date, end_date, country, granularity, **kwargs):
        
        return self.produce('mobile-web', 'average-visit-duration', domain=domain, start_date=start_date, end_date=end_date, country=country, granularity=granularity, **kwargs)
  
    def mobile_bounce_rate(self, domain, start_date, end_date, country, granularity, **kwargs):
        
        return self.produce('mobile-web', 'bounce-rate', domain=domain, start_date=start_date, end_date=end_date, country=country, granularity=granularity, **kwargs)
   
    def mobile_unique_visitors(self, domain, start_date, end_date, country, **kwargs):
        
        return self.produce('unique-visitors', 'mobileweb_unique_visitors', domain=domain, start_date=start_date, end_date=end_date, country=country, **kwargs)
   
    # Desktop Web Traffic Sources
    def desktop_overview_share(self, domain, start_date, end_date, country, **kwargs):
        
        return self.produce('traffic-sources', 'overview', domain=domain, start_date=start_date, end_date=end_date, country=country, **kwargs)
    
    def desktop_search_visits_distribution(self, domain, start_date, end_date, country, **kwargs):
        
        return self.produce('traffic-sources', 'search-visits-distribution', domain=domain, start_date=start_date, end_date=end_date, country=country, **kwargs)
    
    # Mobile Web Traffic Sources
    # Industry Analysis
    # Desktop Keyword Analysis
    # Website Content
    # Other
    def api_lite(self, domain, **kwargs):
        
        return self.produce('general-data', 'all', domain=domain, **kwargs)
    
    # Mobile Apps
    def related_apps(self, domain, Store="Google", **kwargs):

        return self.produce('related-apps', 'related-apps', domain=domain, Store=Store, **kwargs)

    # Mobile Apps Engagement
    def app_dau(self, app_domain, start_date, end_date, country, granularity, **kwargs):
        kwargs["app"] = True 
        return self.produce('engagement', 'dau', domain=app_domain, start_date=start_date, end_date=end_date, country=country, granularity=granularity, **kwargs)
    
    def app_mau(self, app_domain, start_date, end_date, country, granularity, **kwargs):
        kwargs["app"] = True 
        return self.produce('engagement', 'mau', domain=app_domain, start_date=start_date, end_date=end_date, country=country, granularity=granularity, **kwargs)
    
    def app_downloads(self, app_domain, start_date, end_date, country, granularity, **kwargs):
        kwargs["app"] = True 
        return self.produce('engagement', 'downloads', domain=app_domain, start_date=start_date, end_date=end_date, country=country, granularity=granularity, **kwargs)
    
    def app_gender(self, app_domain, country, **kwargs):
        kwargs["app"] = True 
        return self.produce('demographics', 'gender', domain=app_domain, country=country, **kwargs)
    
    def app_age(self, app_domain, country, **kwargs):
        kwargs["app"] = True    
        return self.produce('demographics', 'age', domain=app_domain, country=country, **kwargs)
    
    
    # Sales Solution
    # Segment Analysis
    # Webhookers
    
    def new_vs_returning(self, domain, start_date, end_date, country, **kwargs):
        
        return self.produce('audience', 'new-vs-returning', domain=domain, start_date=start_date, end_date=end_date, country=country, **kwargs)

    def mobile_overview_share(self, domain, start_date, end_date, country, **kwargs):
        
        return self.produce('traffic-sources', 'mobile-overview-share', domain=domain, start_date=start_date, end_date=end_date, country=country, **kwargs)

    def age(self, domain, start_date, end_date, country, **kwargs):
        
        return self.produce('demographics', 'age', domain=domain, start_date=start_date, end_date=end_date, country=country, **kwargs)

    def gender(self, domain, start_date, end_date, country, **kwargs):
        
        return self.produce('demographics', 'gender', domain=domain, start_date=start_date, end_date=end_date, country=country, **kwargs)

    def social(self, domain, start_date, end_date, country, **kwargs):
        
        return self.produce('traffic-sources', 'social', domain=domain, start_date=start_date, end_date=end_date, country=country,**kwargs)

    def overlap(self, domains, start_date, end_date, country, **kwargs):
        kwargs["overlap"] = True
        return self.produce('audience', 'overlap', domains=domains, start_date=start_date, end_date=end_date, country=country, **kwargs)

    def traffic_by_country(self, domain, start_date, end_date, **kwargs):
        
        return self.produce('geo', 'traffic-by-country', domain=domain, start_date=start_date, end_date=end_date, **kwargs)