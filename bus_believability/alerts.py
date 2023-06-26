import spacy
from spacy import displacy
from collections import Counter
import en_core_web_trf
import re

alerts = [
        "Due to a shortage of labour the 715am Route #72 Salmo/Ymir March 3 has been cancelled",
        "Route Cancelled 523am and 1015am #99/20 Connector February 25   Due to a shortage of labour the 523am and 1015am #99/20 Connector February 25 has been cancelled",
        "Route Cancelled 523am, 1015am #99/20 Kootenay Ctr/Slocan Feb 4th   Due to labour shortage the 523 am and 1015 am #99/20 Kootenay Connector / Slocan Valley February 4th have been cancelled.",
        "Route Cancelled 525 pm #99/20 Kootenay Connector/Slocan Jan 20th   Due to labour shortage the 525 pm #99/20 Kootenay Connector / Slocan Valley January 20th has been cancelled.",
        "Route Cancelled 359pm #99/20 Kootenay Ctr/Slocan December 27th   Due to labour shortage the 359 pm #99/20 Kootenay Connector / Slocan Valley December 27th has been cancelled.",
        "Route Cancelled 804 am, 419 pm #99 Kootenay Connector December 9   Due to labour shortage the 804 am, and 419 pm #99 Kootenay Connector December 9th have been cancelled.",
        "ROUTE Cancelled 359pm and 630pm #99/20 Kootenay Connector Sept13   Due to a shortage of labour the 359pm and 630pm #99/20 Kootenay Connector Sept13 has been cancelled",
        "Route Cancelled 523 and 743am #99/20 Connector September 10th   Due to a shortage of labour the 523 and 743am #99/20 Connector September 10th has been cancelled",
        "Route Cancelled 359pm 525pm #99/20 Kootenay Connector Nov 21st   Due to a shortage of labour the 359pm 525pm #99/20 Kootenay Connector Nov 21st has been cancelled"
]

def real_ner():
    ner = en_core_web_trf.load()

    for alert in alerts:
        alert2  = alert.replace('am', ' AM').replace('pm', ' PM')
        print(alert2)
        print('---')
        res = ner(alert2)
        for entity in res.ents:
            print(entity.text, entity.label_)

        print()


def fake_ner():
    for alert in alerts:
        times = re.findall('(\d\d\d\d? ?(?:am|pm)?)', alert)
        times2 = [tm.replace('am','').replace('pm','').replace(':','').replace(' ', '') for tm in times]
        times3 = [(int(tm[:-2]),int(tm[-2:])) for tm in times2]
        print(times3)
        print(re.findall('(Jan|January|Feb|February|Mar|March|Jun|June|Jul|July|Aug|August|Sep|Sept|September|Oct|October|Nov|November|Dec|December) ?(\d\d?) ?(st|nd|rd|th)?', alert))
        print()

fake_ner()
