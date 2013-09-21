class ACSFile :
    name = None
    startPos = 0;
    length = 0;
    
    def __init__(self, fn) :
        self.name = fn;
    
    
class ACSTable :
    name = None
    columns = None
    fileList = None
    desc = None
    subject_area = None
    def __init__(self,tn) :
        self.name = tn
    