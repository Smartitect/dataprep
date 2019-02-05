import re

class NinoValidator:
    def isEmpty(nino):
        return nino == ''
    def isValid(nino):
        return re.match("^(?!BG)(?!GB)(?!NK)(?!KN)(?!TN)(?!NT)(?!ZZ)(?:[A-CEGHJ-PR-TW-Z][A-CEGHJ-NPR-TW-Z])(?:\s*\d\s*){6}([A-D]|\s)$", str(nino))