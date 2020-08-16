import datetime


class Pubmed_paper():
    ''' Used to temporarily store a pubmed paper outside es '''
    def __init__(self):
        self.pm_id = 0
        # every paper has a created_date
        self.created_datetime = datetime.datetime.today()
        self.title = ""
        self.abstract = ""
        self.mesh = ""
        self.keywords = ""

    def __repr__(self):
        return '<Pubmed_paper %r>' % (self.pm_id)