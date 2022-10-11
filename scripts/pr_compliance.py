#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import re
import os
import sys

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #  
#  _____ _ _   _       __     __    _ _     _       _   _                 #
# |_   _(_) |_| | ___  \ \   / /_ _| (_) __| | __ _| |_(_) ___  _ __      #  
#   | | | | __| |/ _ \  \ \ / / _` | | |/ _` |/ _` | __| |/ _ \| '_ \     #
#   | | | | |_| |  __/   \ V / (_| | | | (_| | (_| | |_| | (_) | | | |    #
#   |_| |_|\__|_|\___|    \_/ \__,_|_|_|\__,_|\__,_|\__|_|\___/|_| |_|    #
#                                                                         # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #                                                                 


#validator for titles
def validate_title(title: str):
    return len(re.findall('(\[HUDI\-[0-9]{1,}\]|\[MINOR\])',title)) == 1

#runs an individual title test
#
#   PARAMS
# name: str - the name of the test
# title: str - the title to test
# isTrue: bool - is the title valid
#
#   RETURN
# bool - True if the test passed, False if it failed
def run_title_test(name: str, title: str, isTrue: bool):
    if isTrue != validate_title(title):
        print(f"{name} - FAILED")
        return False
    print(f"{name} - PASSED")
    return True

#tests for title validation
#
#   RETURN
# bool - True if all tests passed, False if any tests fail
def test_title():
    test_return = True
    #test that position doesn't matter for issue
    test_return = run_title_test("issue at front", "[HUDI-1324] my fake pr", True) and test_return
    test_return =  run_title_test("issue in middle", " my [HUDI-1324] fake pr", True) and test_return
    test_return =  run_title_test("issue at end", " my fake pr [HUDI-1324]", True)  and test_return

    #test position doesn't matter for minor
    test_return = run_title_test("minor at front", "[MINOR] my fake pr", True) and test_return
    test_return = run_title_test("minor in middle", " my [MINOR] fake pr", True) and test_return
    test_return = run_title_test("minor at end", " my fake pr [MINOR]", True) and test_return

    #test that more than 4 nums is also ok
    test_return = run_title_test("more than 4 nums in issue", "[HUDI-12345] my fake pr", True) and test_return

    #test that 1 nums is also ok
    test_return = run_title_test("1 num in issue", "[HUDI-1] my fake pr", True) and test_return

    #no nums not ok
    test_return = run_title_test("no nums in issue", "[HUDI-] my fake pr", False) and test_return

    #no brackets not ok
    test_return = run_title_test("no brackets around issue", "HUDI-1234 my fake pr", False) and test_return
    test_return = run_title_test("no brackets around minor", "MINOR my fake pr", False) and test_return
    
    #lowercase not ok
    test_return = run_title_test("lowercase hudi", "[hudi-1234] my fake pr", False) and test_return
    test_return = run_title_test("lowercase minor", "[minor] my fake pr", False) and test_return
    
    #duplicate not ok
    test_return = run_title_test("duplicate issue", "[HUDI-1324][HUDI-1324] my fake pr", False) and test_return
    test_return = run_title_test("duplicate minor", "[MINOR] my fake pr [MINOR]", False) and test_return
    
    #hudi and minor not ok
    test_return = run_title_test("issue and minor", "[HUDI-1324] my [MINOR]fake pr", False) and test_return
    print("*****")
    if test_return:
        print("All title tests passed")
    else:
        print("Some title tests failed")
    print("*****")

    return test_return


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#  ____            _        __     __    _ _     _       _   _                #
# | __ )  ___   __| |_   _  \ \   / /_ _| (_) __| | __ _| |_(_) ___  _ __     #
# |  _ \ / _ \ / _` | | | |  \ \ / / _` | | |/ _` |/ _` | __| |/ _ \| '_ \    #
# | |_) | (_) | (_| | |_| |   \ V / (_| | | | (_| | (_| | |_| | (_) | | | |   #
# |____/ \___/ \__,_|\__, |    \_/ \__,_|_|_|\__,_|\__,_|\__|_|\___/|_| |_|   #
#                    |___/                                                    #
#                                                                             #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #                                            
    
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
#   PARAMS
# name: str - name of the parse section
# identifier: str - line that signifies the start of a section
# linesAfter: set of str - default lines in the template that we ignore when 
#                          verifying that the user filled out the section
# nextSection: str - The name of the next section or "SUCCESS" if this is the 
#                    last section
class ParseSectionData:
    def __init__(self, name: str, identifier: str, linesAfter, nextSection: str):
        self.name = name
        self.identifier = identifier
        self.linesAfter = linesAfter
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
#   PARAMS
# psd: list of ParseSectionData - a list of the data for all the parse sections
class ParseSections:
    def __init__(self, psd):
        self.sections = {}
        for x in psd:
            self.sections[x.name] = x
    
    #returns true if line is an identifier for a section that is not value
    #   PARAMS
    # line: str - the line that we are parsing
    # value: str - the name of the parse section that we are parsing
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
        "CHECKLIST")
    checklist = ParseSectionData("CHECKLIST",
        "### Contributor's checklist",
        {},
        "SUCCESS")
    parseSections = ParseSections([changelogs, impact, risklevel, checklist])

    return ValidateBody(body, "CHANGELOGS", parseSections, debug)


#takes a list of strings and returns a string of those lines separated by \n
def joinLines(lines):
    return "\n".join(lines)

#runs a test for parsing the body
#   PARAMS
# name: str - the name of the test
# body: str - the body to parse
# isTrue: bool - True if the body complies with our validation rules
# debug: bool - True if we want to print debug information
def run_test(name: str, body: str, isTrue: bool, debug: bool):
    validator = make_default_validator(body, debug)
    if isTrue != validator.validate():
        print(f"{name} - FAILED")
        return False
    print(f"{name} - PASSED")
    return True

# Given a list of sections which are lists of strings, it combines them into one
# giant string that is the body to be parsed
def build_body(sections):
    res = ""
    for s in sections:
        res += joinLines(s) + "\n"
    return res

# Tests for validating the body of a pr. Returns true if all tests pass
def test_body():
    DEBUG_MESSAGES = False
    #Create sections that we will combine to create bodies to test validation on
    template_changelogs = [
         "### Change Logs",
        "",
        "_Describe context and summary for this change. Highlight if any code was copied._",
        ""
    ]

    good_changelogs = template_changelogs.copy()
    good_changelogs[1] = "changelogs description"

    template_impact = [
        "### Impact",
        "",
        "_Describe any public API or user-facing feature change or any performance impact._",
        ""
    ]

    good_impact = template_impact.copy()
    good_impact[1] = "impact description"

    template_risklevel = [
        "**Risk level: none | low | medium | high**",
        "",
        "_Choose one. If medium or high, explain what verification was done to mitigate the risks._",
        ""
    ]

    good_risklevel_none = template_risklevel.copy()
    good_risklevel_none[0] = "**Risk level: none**"

    good_risklevel_medium = template_risklevel.copy()
    good_risklevel_medium[0] = "**Risk level: medium**"
    good_risklevel_medium[1] = "risklevel description" 

    template_checklist = [
        "### Contributor's checklist",
        "",
        "- [ ] Read through [contributor's guide](https://hudi.apache.org/contribute/how-to-contribute)",
        "- [ ] Change Logs and Impact were stated clearly",
        "- [ ] Adequate tests were added if applicable",
        "- [ ] CI passed"
    ]

    #list of sections that when combined form a valid body
    good_sections = [good_changelogs, good_impact, good_risklevel_none, template_checklist]

    #list of sections that when combined form the template
    template_sections = [template_changelogs, template_impact, template_risklevel, template_checklist]

    tests_passed = True
    #Test section not filled out
    for i in range(len(good_sections)-1):
        test_sections = []
        for j in range(len(good_sections)):
            if j != i:
                test_sections.append(good_sections[j].copy())
            else:
                test_sections.append(template_sections[j].copy())
        tests_passed = run_test(f"template section not filled out: {i}", build_body(test_sections), False, DEBUG_MESSAGES) and tests_passed

    #Test duplicate section
    for i in range(len(good_sections)-1):
        test_sections = []
        for j in range(len(good_sections)):
            test_sections.append(good_sections[j].copy())
            if j == i:
                test_sections.append(good_sections[j].copy())
        tests_passed = run_test(f"duplicate section: {i}", build_body(test_sections), False, DEBUG_MESSAGES) and tests_passed
    
    #Test out of order section
    for i in range(len(good_sections)):
        test_sections = []
        for j in range(len(good_sections)):
            test_sections.append(good_sections[j].copy())
        k = (i + 3) % len(good_sections)
        test_sections[i], test_sections[k] = test_sections[k],test_sections[i]
        tests_passed = run_test(f"Swapped sections: {i}, {k}", build_body(test_sections), False, DEBUG_MESSAGES) and tests_passed

    #Test missing section
    for i in range(len(good_sections)):
        test_sections = []
        for j in range(len(good_sections)):
            if i != j:
                test_sections.append(good_sections[j].copy())
        tests_passed = run_test(f"Missing Section: {i}", build_body(test_sections), False, DEBUG_MESSAGES) and tests_passed
    
    #Test good body with risk level of none:
    tests_passed = run_test("good documentation. risk level none", build_body(good_sections), True, DEBUG_MESSAGES) and tests_passed

    #Test good body with risk level of medium:
    risk_level_index = 2
    good_medium_risk_level = good_sections.copy()
    good_medium_risk_level[risk_level_index] = good_risklevel_medium
    tests_passed = run_test("good documentation. risk level medium", build_body(good_medium_risk_level), True, DEBUG_MESSAGES) and tests_passed

    #Test good body except medium risk level and there is no description
    bad_medium_risk_level = good_sections.copy()
    bad_risklevel_medium = good_risklevel_medium.copy()
    bad_risklevel_medium[1] = ""
    bad_medium_risk_level[risk_level_index] = bad_risklevel_medium
    tests_passed = run_test("medium risk level but no description", build_body(bad_medium_risk_level), False, DEBUG_MESSAGES) and tests_passed

    #Test unknown risk level:
    unknow_risk_level = good_sections.copy()
    unknow_risk_level_section = template_risklevel.copy()
    unknow_risk_level_section[0] = "**Risk level: unknown**"
    unknow_risk_level[risk_level_index] = unknow_risk_level_section
    tests_passed = run_test("unknown risk level", build_body(unknow_risk_level), False, DEBUG_MESSAGES) and tests_passed

    print("*****")
    if tests_passed:
        print("All body tests passed")
    else:
        print("Some body tests failed")
    print("*****")
    
    return tests_passed





if __name__ == '__main__':
    if len(sys.argv) > 1:
        title_tests = test_title()
        body_tests = test_body()
        if title_tests and body_tests:
            exit(0)
        else:
            exit(-1)


    title = os.getenv("REQUEST_TITLE")
    body = os.getenv("REQUEST_BODY")

    if title is None:
        print("no title")
        exit(-1)

    if not validate_title(title):
        print("invalid title")
        exit(-1)

    if body is None:
        print("no pr body")
        exit(-1)

    validator = make_default_validator(body,True)
    if not validator.validate():
        exit(-1)
    exit(0)
