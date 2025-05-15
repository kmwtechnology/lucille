def process(doc):
    import datetime
    now = datetime.datetime.now().isoformat()
    doc['current_time'] = now
    return doc
