import re
import os

def title_is_ok(title):
    return len(re.findall('(\[HUDI\-[0-9]{1,}\]|\[MINOR\])',title)) == 1

def test_title():
    #test that position doesn't matter
    assert title_is_ok("[HUDI-1324] my fake pr")
    assert title_is_ok(" my [HUDI-1324] fake pr")
    assert title_is_ok(" my fake pr [HUDI-1324]")
    #test position doesn't matter for minor
    assert title_is_ok("[MINOR] my fake pr")
    assert title_is_ok(" my [MINOR] fake pr")
    assert title_is_ok(" my fake pr [MINOR]")
    #test that more than 4 nums is also ok
    assert title_is_ok("[HUDI-12345] my fake pr")
    #test that 1 nums is also ok
    assert title_is_ok("[HUDI-1] my fake pr")

    #no nums not ok
    assert not title_is_ok("[HUDI-] my fake pr")
    #no brackets not ok
    assert not title_is_ok("HUDI-1234 my fake pr")
    assert not title_is_ok("MINOR my fake pr")
    #lowercase not ok
    assert not title_is_ok("[hudi-1234] my fake pr")
    assert not title_is_ok("[minor] my fake pr")
    #duplicate not ok
    assert not title_is_ok("[HUDI-1324][HUDI-1324] my fake pr")
    #hudi and minor not ok
    assert not title_is_ok("[HUDI-1324] my [MINOR]fake pr")



class Outcomes:
    ERROR = 0
    CONTINUE = 1
    NEXTSECTION = 2
    SUCCESS = 3


class ParseSectionData:
    def __init__(self, name: str, identifier: str, linesAfter, nextSection: str):
        self.name = name
        self.identifier = identifier
        self.linesAfter = linesAfter
        self.nextSection = nextSection
    
    def identify(self, line: str):
        return line == self.identifier
    
    def identifyAfter(self, line: str):
        return line not in self.linesAfter

class RiskLevelData(ParseSectionData):
    def __init__(self, name: str, identifier: str, linesAfter, nextSection: str):
        super().__init__(name, identifier, linesAfter, nextSection)
    
    def identify(self, line: str):
        return line.startswith(self.identifier)


class ParseSections:
    def __init__(self, psd):
        self.sections = {}
        for x in psd:
            self.sections[x.name] = x
    
    def validateOthers(self, line: str, value: str):
        for section in self.sections:
            if section.name != value:
                if section.identify(line):
                    return True
        return False    
    
    def get(self, name):
        return self.sections.get(name)

class ParseSection:
    def __init__(self, data: ParseSectionData, sections: ParseSections, debug=False):
        self.found = False
        self.debug = debug
        self.data = data
        self.sections = sections

    def error(self, message):
        if self.debug:
            print(f"ERROR:(state: {self.data.name}, found: {self.found}, message: {message}")

    def validateAfter(self, line):
        return self.found and self.data.identifyAfter(line)

    def processIdentify(self, line):
        if self.found:
            self.error(f"duplicate line \"{line}\"")
            return Outcomes.ERROR
        self.found = True
        return Outcomes.CONTINUE
    
    def validateLine(self,line):
        # see if it matches template
        if self.data.identify(line):
            o = self.processIdentify(line)
            if o == Outcomes.NEXTSECTION and self.data.nextSection == "SUCCESS":
                return Outcomes.SUCCESS
            return o
        elif self.sections.validateOthers(line, self.data.name):
            self.error(f"Out of order section or missing description \"{line}\"")
            return Outcomes.ERROR
        elif self.validateAfter(line):
            return Outcomes.NEXTSECTION
        return Outcomes.CONTINUE

class RiskLevelSection(ParseSection):
    def __init__(self, data: ParseSectionData, sections: ParseSections, debug=False):
        super().__init__(data, sections, debug)

    def processIdentify(self, line):
        if self.found:
            self.error(f"duplicate line starting with \"{self.identifier}\"")
            return Outcomes.ERROR
        if line == "**Risk level: none | low | medium | high**":
            self.error("risk level not chosen")
            return Outcomes.ERROR
        # an explanation is not required for none or low
        if "NONE" in line.upper() or "LOW" in line.upper():
            return Outcomes.NEXTSECTION
        elif "MEDIUM" in line.upper() or "HIGH" in line.upper():
            # an explanation is required so we don't change state
            self.found = True
            return Outcomes.CONTINUE
        else:
            #they put something weird in for risk level
            self.error("invalid choice for risk level")
            return Outcomes.ERROR


class ValidateBody:
    def __init__(self, body: str, firstSection: str, sections: ParseSections, debug=False):
        self.body = body
        self.debug = debug
        self.firstSection = firstSection
        self.section = None
        self.sections = sections

    def nextSection(self):
        if self.section is None:
            data = self.sections.get(self.firstSection)
        else:
            data = self.sections.get(self.section.nextSection)
        if data.name == "RISKLEVEL":
            self.section = RiskLevelSection(data=data, sections=self.sections, debug=self.debug)
        else:
            self.section = ParseSection(data=data, sections=self.sections, debug=self.debug)

    def validate(self):
        self.nextSection()
        for line in self.body.splitlines():
            if len(line) == 0:
                continue
            o = self.section.validateLine(line)
            if o == Outcomes.ERROR:
                return False
            elif o == Outcomes.SUCCESS:
                return True
            elif o == Outcomes.NEXTSECTION:
                self.nextSection()
        self.section.error("template is not filled out properly")
        return False
        

def make_default_validator(body, debug=False):
    changelogs = ParseSectionData("CHANGELOGS",
        "### Change Logs",
        {"_Describe context and summary for this change. Highlight if any code was copied._"},
        "IMPACT")
    impact = ParseSectionData("IMPACT",
        "### Impact",
        {"_Describe any public API or user-facing feature change or any performance impact._"},
        "RISKLEVEL")
    risklevel = RiskLevelData("RISKLEVEL",
        "**Risk level:",
        {"_Choose one. If medium or high, explain what verification was done to mitigate the risks._"},
        "DOCUMENTATION")
    documentationUpdate = ParseSectionData("DOCUMENTATION",
        "### Documentation Update",
        {"_Describe any necessary documentation update if there is any new feature, config, or user-facing change_",
        "- _The config description must be updated if new configs are added or the default value of the configs are changed_",
        "- _Any new feature or user-facing change requires updating the Hudi website. Please create a Jira ticket, attach the",
       "ticket number here and follow the [instruction](https://hudi.apache.org/contribute/developer-setup#website) to make",
       "changes to the website._"},
       "CHECKLIST")
    checklist = ParseSectionData("CHECKLIST",
        "### Contributor's checklist",
        {},
        "SUCCESS")
    parseSections = ParseSections([changelogs, impact, risklevel, documentationUpdate, checklist])

    return ValidateBody(body, "CHANGELOGS", parseSections, debug)


# def joinLines(lines):
#     return "\n".join(lines)

# def run_test(name, body, isTrue, debug):
#     print(name)
#     validator = make_default_validator(body, debug)
#     if isTrue:
#         assert validator.validate()
#     else:
#         assert not validator.validate()


# def test_body():
#     template_changelogs = [
#          "### Change Logs",
#         "",
#         "_Describe context and summary for this change. Highlight if any code was copied._",
#         ""
#     ]

#     template_impact = [
#         "### Impact",
#         "",
#         "_Describe any public API or user-facing feature change or any performance impact._",
#         ""
#     ]

#     template_risklevel = [
#         "**Risk level: none | low | medium | high**",
#         "",
#         "_Choose one. If medium or high, explain what verification was done to mitigate the risks._",
#         ""
#     ]

#     template_checklist = [
#         "### Contributor's checklist",
#         "",
#         "- [ ] Read through [contributor's guide](https://hudi.apache.org/contribute/how-to-contribute)",
#         "- [ ] Change Logs and Impact were stated clearly",
#         "- [ ] Adequate tests were added if applicable",
#         "- [ ] CI passed"
#     ]


#     duplicate_change_logs = [
#         "### Change Logs",
#         "",
#         "### Change Logs",
#         "_Describe context and summary for this change. Highlight if any code was copied._",
#         "",
#         "### Impact",
#         "",
#         "_Describe any public API or user-facing feature change or any performance impact._",
#         "",
#         "**Risk level: none | low | medium | high**",
#         "",
#         "_Choose one. If medium or high, explain what verification was done to mitigate the risks._",
#         "",
#         "### Contributor's checklist",
#         "",
#         "- [ ] Read through [contributor's guide](https://hudi.apache.org/contribute/how-to-contribute)",
#         "- [ ] Change Logs and Impact were stated clearly",
#         "- [ ] Adequate tests were added if applicable",
#         "- [ ] CI passed"
#         ]

#     print("duplicate_change_logs")
#     assert not body_is_ok("\n".join(duplicate_change_logs),debug_messages)

#     describe_context = [
#         "### Change Logs",
#         "",
#         "describing context",
#         "",
#         "### Impact",
#         "",
#         "_Describe any public API or user-facing feature change or any performance impact._",
#         "",
#         "**Risk level: none | low | medium | high**",
#         "",
#         "_Choose one. If medium or high, explain what verification was done to mitigate the risks._",
#         "",
#         "### Contributor's checklist",
#         "",
#         "- [ ] Read through [contributor's guide](https://hudi.apache.org/contribute/how-to-contribute)",
#         "- [ ] Change Logs and Impact were stated clearly",
#         "- [ ] Adequate tests were added if applicable",
#         "- [ ] CI passed"
#         ]
#     print("describe_context")
#     assert not body_is_ok("\n".join(describe_context),debug_messages)

#     describe_impact = [
#         "### Change Logs",
#         "",
#         "_Describe context and summary for this change. Highlight if any code was copied._",
#         "describing the context",
#         "",
#         "### Impact",
#         "",
#         "describing impact",
#         "",
#         "**Risk level: none | low | medium | high**",
#         "",
#         "_Choose one. If medium or high, explain what verification was done to mitigate the risks._",
#         "",
#         "### Contributor's checklist",
#         "",
#         "- [ ] Read through [contributor's guide](https://hudi.apache.org/contribute/how-to-contribute)",
#         "- [ ] Change Logs and Impact were stated clearly",
#         "- [ ] Adequate tests were added if applicable",
#         "- [ ] CI passed"
#         ]
#     print("describe_impact")
#     assert not body_is_ok("\n".join(describe_impact),debug_messages)

#     duplicate_impact = [
#         "### Change Logs",
#         "",
#         "_Describe context and summary for this change. Highlight if any code was copied._",
#         "describing the context",
#         "",
#         "### Impact",
#         "### Impact",
#         "",
#         "describing impact",
#         "",
#         "**Risk level: none | low | medium | high**",
#         "",
#         "_Choose one. If medium or high, explain what verification was done to mitigate the risks._",
#         "",
#         "### Contributor's checklist",
#         "",
#         "- [ ] Read through [contributor's guide](https://hudi.apache.org/contribute/how-to-contribute)",
#         "- [ ] Change Logs and Impact were stated clearly",
#         "- [ ] Adequate tests were added if applicable",
#         "- [ ] CI passed"
#         ]
#     print("duplicate_impact")
#     assert not body_is_ok("\n".join(duplicate_impact),debug_messages)

#     choose_risk_level_none = [
#         "### Change Logs",
#         "",
#         "_Describe context and summary for this change. Highlight if any code was copied._",
#         "describing context",
#         "",
#         "### Impact",
#         "describing impact",
#         "",
#         "_Describe any public API or user-facing feature change or any performance impact._",
#         "",
#         "**Risk level: none**",
#         "",
#         "_Choose one. If medium or high, explain what verification was done to mitigate the risks._",
#         "",
#         "### Contributor's checklist",
#         "",
#         "- [ ] Read through [contributor's guide](https://hudi.apache.org/contribute/how-to-contribute)",
#         "- [ ] Change Logs and Impact were stated clearly",
#         "- [ ] Adequate tests were added if applicable",
#         "- [ ] CI passed"
#         ]
#     print("choose_risk_level_none")
#     assert body_is_ok("\n".join(choose_risk_level_none),debug_messages)

#     duplicate_risk_level = [
#         "### Change Logs",
#         "",
#         "_Describe context and summary for this change. Highlight if any code was copied._",
#         "describing context",
#         "",
#         "### Impact",
#         "describing impact",
#         "",
#         "_Describe any public API or user-facing feature change or any performance impact._",
#         "",
#         "**Risk level: none**",
#         "",
#         "**Risk level: none | low | medium | high**",
#         "_Choose one. If medium or high, explain what verification was done to mitigate the risks._",
#         "",
#         "### Contributor's checklist",
#         "",
#         "- [ ] Read through [contributor's guide](https://hudi.apache.org/contribute/how-to-contribute)",
#         "- [ ] Change Logs and Impact were stated clearly",
#         "- [ ] Adequate tests were added if applicable",
#         "- [ ] CI passed"
#         ]
#     print("duplicate_risk_level")
#     assert not body_is_ok("\n".join(duplicate_risk_level),debug_messages)


#     choose_risk_level_high = [
#         "### Change Logs",
#         "",
#         "_Describe context and summary for this change. Highlight if any code was copied._",
#         "describing context",
#         "",
#         "### Impact",
#         "",
#         "describing impact",
#         "_Describe any public API or user-facing feature change or any performance impact._",
#         "",
#         "**Risk level: high**",
#         "",
#          "_Choose one. If medium or high, explain what verification was done to mitigate the risks._",
#         "",
#         "### Contributor's checklist",
#         "",
#         "- [ ] Read through [contributor's guide](https://hudi.apache.org/contribute/how-to-contribute)",
#         "- [ ] Change Logs and Impact were stated clearly",
#         "- [ ] Adequate tests were added if applicable",
#         "- [ ] CI passed"
#         ]
#     print("choose_risk_level_high")
#     assert not body_is_ok("\n".join(choose_risk_level_high),debug_messages)

#     no_risk_level = [
#         "### Change Logs",
#         "",
#         "_Describe context and summary for this change. Highlight if any code was copied._",
#         "describing context",
#         "",
#         "### Impact",
#         "",
#         "describing impact",
#         "_Describe any public API or user-facing feature change or any performance impact._",
#         "",
#         "**Risk level: **",
#         "",
#          "_Choose one. If medium or high, explain what verification was done to mitigate the risks._",
#         "",
#         "### Contributor's checklist",
#         "",
#         "- [ ] Read through [contributor's guide](https://hudi.apache.org/contribute/how-to-contribute)",
#         "- [ ] Change Logs and Impact were stated clearly",
#         "- [ ] Adequate tests were added if applicable",
#         "- [ ] CI passed"
#         ]
#     print("no_risk_level")
#     assert not body_is_ok("\n".join(no_risk_level),debug_messages)

#     weird_risk_level = [
#         "### Change Logs",
#         "",
#         "_Describe context and summary for this change. Highlight if any code was copied._",
#         "describing context",
#         "",
#         "### Impact",
#         "",
#         "describing impact",
#         "_Describe any public API or user-facing feature change or any performance impact._",
#         "",
#         "**Risk level: onehouse**",
#         "",
#          "_Choose one. If medium or high, explain what verification was done to mitigate the risks._",
#         "",
#         "### Contributor's checklist",
#         "",
#         "- [ ] Read through [contributor's guide](https://hudi.apache.org/contribute/how-to-contribute)",
#         "- [ ] Change Logs and Impact were stated clearly",
#         "- [ ] Adequate tests were added if applicable",
#         "- [ ] CI passed"
#         ]
#     print("weird_risk_level")
#     assert not body_is_ok("\n".join(weird_risk_level),debug_messages)

#     choose_risk_level_high_explained = [
#         "### Change Logs",
#         "",
#         "_Describe context and summary for this change. Highlight if any code was copied._",
#         "describing context",
#         "",
#         "### Impact",
#         "",
#         "describing impact",
#         "_Describe any public API or user-facing feature change or any performance impact._",
#         "",
#         "**Risk level: high**",
#         "",
#          "_Choose one. If medium or high, explain what verification was done to mitigate the risks._",
#          "explaining my impact",
#         "",
#         "### Contributor's checklist",
#         "",
#         "- [ ] Read through [contributor's guide](https://hudi.apache.org/contribute/how-to-contribute)",
#         "- [ ] Change Logs and Impact were stated clearly",
#         "- [ ] Adequate tests were added if applicable",
#         "- [ ] CI passed"
#         ]
#     print("choose_risk_level_high_explained")
#     assert body_is_ok("\n".join(choose_risk_level_high_explained),debug_messages)

#     good_documentation = [
#         "### Change Logs",
#         "",
#         "_Describe context and summary for this change. Highlight if any code was copied._",
#         "actually describe context",
#         "",
#         "### Impact",
#         "",
#         "actually describe impact",
#         "_Describe any public API or user-facing feature change or any performance impact._",
#         "",
#         "**Risk level: medium **",
#         "",
#         "_Choose one. If medium or high, explain what verification was done to mitigate the risks._",
#         "",
#         "Risk is medium so we need to describe it",
#         "### Contributor's checklist",
#         "",
#         "- [ ] Read through [contributor's guide](https://hudi.apache.org/contribute/how-to-contribute)",
#         "- [ ] Change Logs and Impact were stated clearly",
#         "- [ ] Adequate tests were added if applicable",
#         "- [ ] CI passed"
#         ]
#     print("good_documentation")
#     assert body_is_ok("\n".join(good_documentation),debug_messages)

#     good_documentation_no_describe_risk = [
#         "### Change Logs",
#         "",
#         "_Describe context and summary for this change. Highlight if any code was copied._",
#         "actually describe context",
#         "",
#         "### Impact",
#         "",
#         "actually describe impact",
#         "_Describe any public API or user-facing feature change or any performance impact._",
#         "",
#         "**Risk level: low **",
#         "",
#         "_Choose one. If medium or high, explain what verification was done to mitigate the risks._",
#         "",
#         "### Contributor's checklist",
#         "",
#         "- [ ] Read through [contributor's guide](https://hudi.apache.org/contribute/how-to-contribute)",
#         "- [ ] Change Logs and Impact were stated clearly",
#         "- [ ] Adequate tests were added if applicable",
#         "- [ ] CI passed"
#         ]
#     print("good_documentation_no_describe_risk")
#     assert body_is_ok("\n".join(good_documentation_no_describe_risk),debug_messages)

#     no_checklist = [
#         "### Change Logs",
#         "",
#         "_Describe context and summary for this change. Highlight if any code was copied._",
#         "actually describe context",
#         "",
#         "### Impact",
#         "",
#         "actually describe impact",
#         "_Describe any public API or user-facing feature change or any performance impact._",
#         "",
#         "**Risk level: medium **",
#         "",
#         "_Choose one. If medium or high, explain what verification was done to mitigate the risks._",
#         "",
#         "Risk is medium so we need to describe it",
#         ]
#     print("no_checklist")
#     assert not body_is_ok("\n".join(no_checklist),debug_messages)



if __name__ == '__main__':
    title = os.getenv("REQUEST_TITLE")
    body = os.getenv("REQUEST_BODY")
    if not title_is_ok(title):
        exit(-1)

    validator = make_default_validator(body,True)
    
    if not validator.validate():
        exit(-2)
    exit(0)