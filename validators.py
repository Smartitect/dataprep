import re

class NinoValidator:
    def isEmpty(nino):
        return nino == ''
    def isValid(nino):
        # http://www.regexlib.com/REDetails.aspx?regexp_id=527
        return re.match("^[ABCEGHJKLMNOPRSTWXYZabceghjklmnoprstwxyz][ABCEGHJKLMNPRSTWXYZabceghjklmnprstwxyz][0-9]{6}[A-D\sa-d]{0,1}$", str(nino))