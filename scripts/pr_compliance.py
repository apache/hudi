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


#Enums for the the outcome of parsing a single line
class Outcomes:
    #error was found so we should stop parsing and exit with error
    ERROR = 0

    #continue to parse the next line
    CONTINUE = 1

    #All requirements for the current section have been met so we should start 
    #parsing the next section
    NEXTSECTION = 2

    #parsing has concluded succesfully, exit with no error
    SUCCESS = 3


#Holds the data for a section
class ParseSectionData:
    def __init__(self, name: str, identifier: str, linesAfter, nextSection: str):
        #name of the parse section
        self.name = name

        #line that signifies the start of a section
        self.identifier = identifier

        #default line in the template that we ignore when verifying that the
        #user filled out the section
        self.linesAfter = linesAfter

        #The name of the next section or "SUCCESS" if this is the last section
        self.nextSection = nextSection
    
    #returns true if line matches the identifier
    def identify(self, line: str):
        return line == self.identifier
    
    #returns true if user has added new text to the section
    def identifyAfter(self, line: str):
        return line not in self.linesAfter


#Special holder of data for risk level because the identifier line is modified
#by the user
class RiskLevelData(ParseSectionData):
    def __init__(self, name: str, identifier: str, linesAfter, nextSection: str):
        super().__init__(name, identifier, linesAfter, nextSection)
    
    #we check that the line start with the identifier because the identifier 
    #line will be modified when filled out correctly
    def identify(self, line: str):
        return line.startswith(self.identifier)

#Holds all the parse section data in a map
class ParseSections:
    def __init__(self, psd):
        self.sections = {}
        for x in psd:
            self.sections[x.name] = x
    
    #returns true if line is an identifier for a section that is not value
    def validateOthers(self, line: str, value: str):
        for name in self.sections:
            if name != value:
                if self.sections[name].identify(line):
                    return True
        return False    
    
    #returns the ParseSectionData that is named name
    def get(self, name):
        return self.sections.get(name)


#Main class used to parse a section
class ParseSection:
    def __init__(self, data: ParseSectionData, sections: ParseSections, debug=False):
        #set to true when the sections identifier is found
        self.found = False

        #true if debug messages should be printed
        self.debug = debug

        #the data for this parse section
        self.data = data

        #data of all the parse sections
        self.sections = sections

    #prints error message if debug is set to true
    def error(self, message):
        if self.debug:
            print(f"ERROR:(state: {self.data.name}, found: {self.found}, message: {message}")

    #returns the name of the next section
    def nextSection(self):
        return self.data.nextSection

    #Returns true if we have already found the section identifier and line is 
    #not in the default template
    def validateAfter(self, line):
        return self.found and self.data.identifyAfter(line)

    #Decides what outcome occurs when the section identifier is found
    def processIdentify(self, line):
        if self.found:
            #since we have already found the identifier, this means that we have
            #found a duplicate of the identifier
            self.error(f"duplicate line \"{line}\"")
            return Outcomes.ERROR
        self.found = True
        return Outcomes.CONTINUE
    
    #Decides the outcome state by processing line
    def validateLine(self,line):
        if self.data.identify(line):
            #we have found the identifier so we should decide what to do
            return self.processIdentify(line)
        elif self.sections.validateOthers(line, self.data.name):
            #we have found the identifier for another section
            self.error(f"Out of order section or missing description \"{line}\"")
            return Outcomes.ERROR
        elif self.validateAfter(line):
            #the pr author has added new text to this section so we consider it
            #to be filled out
            if self.nextSection() == "SUCCESS":
                #if next section is "SUCCESS" then there are no more sections 
                #to process
                return Outcomes.SUCCESS
            return Outcomes.NEXTSECTION
        return Outcomes.CONTINUE

#We do not check this section for data
#currently just used for the checklist where they just need to check boxes
class NoDataSection(ParseSection):
    def __init__(self, data: ParseSectionData, sections: ParseSections, debug=False):
        super().__init__(data, sections, debug)

    #After finding the identifier we don't need to look for anything else so we
    #can just go to the next section or terminate if this is the last
    def processIdentify(self, line):
        o = super().processIdentify(line)
        if o  == Outcomes.CONTINUE:
            if self.nextSection() == "SUCCESS":
                return Outcomes.SUCCESS
            else:
                return Outcomes.NEXTSECTION
        return o

#Risk level has different processing because the pr author will modify the 
#identifier and doesn't need to add description if risk is none or low
class RiskLevelSection(ParseSection):
    def __init__(self, data: ParseSectionData, sections: ParseSections, debug=False):
        super().__init__(data, sections, debug)

    def processIdentify(self, line):
        if self.found:
            #since we have already found the identifier, this means that we have
            #found a duplicate of the identifier
            self.error(f"duplicate line starting with \"{self.identifier}\"")
            return Outcomes.ERROR
        if line == "**Risk level: none | low | medium | high**":
            #the user has not modified this line so a risk level was not chosen
            self.error("risk level not chosen")
            return Outcomes.ERROR
        if "NONE" in line.upper() or "LOW" in line.upper():
            # an explanation is not required for none or low so we can just
            # move on to the next section or terminate if this is the last
            if self.nextSection() == "SUCCESS":
                return Outcomes.SUCCESS
            else:
                return Outcomes.NEXTSECTION
        elif "MEDIUM" in line.upper() or "HIGH" in line.upper():
            # an explanation is required so we don't change state
            self.found = True
            return Outcomes.CONTINUE
        else:
            #the pr author put something weird in for risk level
            self.error("invalid choice for risk level")
            return Outcomes.ERROR

#Class that orchestrates the validation of the entire body
class ValidateBody:
    def __init__(self, body: str, firstSection: str, sections: ParseSections, debug=False):
        #the body of the pr post
        self.body = body

        #true if debug messages should be printed
        self.debug = debug

        #the name of the first section of the post
        self.firstSection = firstSection

        #the current section we are processing
        self.section = None

        #ParseSections which holds the data for all the sections
        self.sections = sections

    #Updates self.section to the next section
    def nextSection(self):
        #get the name of the section to parse
        sectionName = self.firstSection
        if self.section is not None:
            sectionName = self.section.nextSection()

        #get the data for that section
        data = self.sections.get(sectionName)
        if data is None:
            print(f"parse section {sectionName} not found")
            exit(-3)
        
        #create the section
        if data.name == "RISKLEVEL":
            self.section = RiskLevelSection(data=data, sections=self.sections, debug=self.debug)
        elif data.name == "CHECKLIST":
            self.section = NoDataSection(data=data, sections=self.sections, debug=self.debug)
        else:
            self.section = ParseSection(data=data, sections=self.sections, debug=self.debug)

    #Returns true if the body complies with the validation rules else false
    def validate(self):
        #instantiate self.section since it starts null
        self.nextSection()

        #validate each line
        for line in self.body.splitlines():
            #ignore empty lines
            if len(line) == 0:
                continue

            #run the parse section validation
            o = self.section.validateLine(line)
            
            #decide what to do based on outcome
            if o == Outcomes.ERROR:
                return False
            elif o == Outcomes.SUCCESS:
                return True
            elif o == Outcomes.NEXTSECTION:
                self.nextSection()
        #if we get through all the lines without a success outcome, then the 
        #body does not comply
        self.section.error("template is not filled out properly")
        return False
        
#Generate the validator for the current template.
#needs to be manually updated
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