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
import inspect
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
class ParseSectionData:
    def __init__(self, name: str, identifier: str, linesAfter: str):
        self.name = name
        self.identifier = identifier
        self.linesAfter = linesAfter
        self.prevSection = ""
        self.nextSection = ""
        
        
    
    #returns true if line matches the identifier
    def identify(self, line: str):
        return line == self.identifier
    
    #returns true if user has added new text to the section
    def identifyAfter(self, line: str):
        return line not in self.linesAfter


#Special holder of data for risk level because the identifier line is modified
#by the user
class RiskLevelData(ParseSectionData):
    def __init__(self, name: str, identifier: str, linesAfter):
        super().__init__(name, identifier, linesAfter)
    
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
        assert len(psd) > 0
        for i in range(len(psd)):
            prevI = i - 1
            nextI = i + 1
            if prevI < 0:
                psd[i].prevSection = "START"
            else:
                psd[i].prevSection = psd[prevI].name
            
            if nextI >= len(psd):
                psd[i].nextSection = "END"
            else:
                psd[i].nextSection = psd[nextI].name
            
            self.sections[psd[i].name] = psd[i]
            

    
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
    
    #gets the name of the section identified in the line
    #   PARAMS
    # line: str - the line that we are parsing
    #   RETURN
    # string - name of the section if the identifier is found, else none
    def getSectionName(self, line: str):
        for name in self.sections:
            if self.sections[name].identify(line):
                return name
        return None    


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
    def error(self, line: str, lineno: str, message: str):
        if self.debug:
            pyline = inspect.getframeinfo(inspect.stack()[1][0]).lineno
            print(f"::error file=pr_compliance.py,line={pyline}::{message}")
            if lineno != "" and line != "":
                print(f"::error file=pr_compliance.py,line={pyline}::found on line {lineno}: {line}")
            print(f"::debug::state: {self.data.name}, found: {self.found},")

    #returns the name of the next section
    def nextSection(self):
        return self.data.nextSection

    #Returns true if we have already found the section identifier and line is 
    #not in the default template
    def validateAfter(self, line):
        return self.found and self.data.identifyAfter(line)

    #Decides what outcome occurs when the section identifier is found
    def processIdentify(self, line, lineno):
        if self.found:
            #since we have already found the identifier, this means that we have
            #found a duplicate of the identifier
            self.error(line, lineno, f"Duplicate {self.data.name} section found")
            return Outcomes.ERROR
        self.found = True
        return Outcomes.CONTINUE

    def makeValidateOthersErrorMessage(self, line):
        if self.found:
            if self.nextSection() != "END" and self.sections.sections[self.nextSection()].identify(line):
                #we found the next identifier but haven't found a description
                #yet for this section
                return f"Section {self.data.name} is missing a description"
            #we found a section other than the next section
            return f"Section {self.data.name} should be followed by section {self.data.nextSection}"

        #section identifier has not been found yet
        sectionFound = self.sections.getSectionName(line)
        if sectionFound is None:
            print("ERROR: none found even though validateOthers returned True")
            exit(1)
        elif sectionFound == self.data.prevSection:
            #we have not found the current section identifier but we found the
            #previous section identifier again
            return f"Duplicate {self.data.prevSection} section found"
         
        if self.data.prevSection == "START":
            return f"Section {self.data.name} should be the first section"
        if sectionFound == self.data.nextSection:
            return f"Missing section {self.data.name} between {self.data.prevSection} and {self.data.nextSection}"
        return f"Section {self.data.name} was expected after section {self.data.prevSection}"
    
    #Decides the outcome state by processing line
    def validateLine(self,line,lineno):
        if self.data.identify(line):
            #we have found the identifier so we should decide what to do
            return self.processIdentify(line,lineno)
        elif self.sections.validateOthers(line, self.data.name):
            #we have found the identifier for another section
            #figure out what the error is
            self.error(line,lineno,self.makeValidateOthersErrorMessage(line))
            return Outcomes.ERROR
        elif self.validateAfter(line):
            #the pr author has added new text to this section so we consider it
            #to be filled out
            if self.nextSection() == "END":
                #if next section is "END" then there are no more sections 
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
    def processIdentify(self, line, lineno):
        o = super().processIdentify(line, lineno)
        if o  == Outcomes.CONTINUE:
            if self.nextSection() == "END":
                return Outcomes.SUCCESS
            else:
                return Outcomes.NEXTSECTION
        return o

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
            print(f"ERROR with your parse section setup. Parse section {sectionName} not found")
            exit(-3)
        
        #create the section
        if data.name == "CHECKLIST":
            self.section = NoDataSection(data=data, sections=self.sections, debug=self.debug)
        else:
            self.section = ParseSection(data=data, sections=self.sections, debug=self.debug)

    #Returns true if the body complies with the validation rules else false
    def validate(self):
        #instantiate self.section since it starts null
        self.nextSection()

        #validate each line
        for lineno, line in enumerate(self.body.splitlines(), 1):
            #ignore empty lines
            if len(line) == 0:
                continue

            #run the parse section validation
            o = self.section.validateLine(line, lineno)
            
            #decide what to do based on outcome
            if o == Outcomes.ERROR:
                return False
            elif o == Outcomes.SUCCESS:
                return True
            elif o == Outcomes.NEXTSECTION:
                self.nextSection()
        #if we get through all the lines without a success outcome, then the 
        #body does not comply
        if self.section.data.nextSection == "END":
            if self.section.found:
                self.section.error("","",f"Section {self.section.data.name} is missing a description")
                return False
            self.section.error("","",f"Missing section {self.section.data.name} at the end")
            return False
        self.section.error("","", "Please make sure you have filled out the template correctly. You can find a blank template in /.github/PULL_REQUEST_TEMPLATE.md")
        return False
        
#Generate the validator for the current template.
#needs to be manually updated
def make_default_validator(body, debug=False):
    changelogs = ParseSectionData("CHANGELOGS",
        "### Change Logs",
        {"_Describe context and summary for this change. Highlight if any code was copied._"})
    impact = ParseSectionData("IMPACT",
        "### Impact",
        {"_Describe any public API or user-facing feature change or any performance impact._"})
    risklevel = RiskLevelData("RISKLEVEL",
        "### Risk level",
        {"_If medium or high, explain what verification was done to mitigate the risks._"})
    checklist = ParseSectionData("CHECKLIST",
        "### Contributor's checklist",
        {})
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
        "### Risk level (write none, low medium or high below)",
        "",
        "_If medium or high, explain what verification was done to mitigate the risks._",
        ""
    ]

    good_risklevel = template_risklevel.copy()
    good_risklevel[1] = "none"

    template_checklist = [
        "### Contributor's checklist",
        "",
        "- [ ] Read through [contributor's guide](https://hudi.apache.org/contribute/how-to-contribute)",
        "- [ ] Change Logs and Impact were stated clearly",
        "- [ ] Adequate tests were added if applicable",
        "- [ ] CI passed"
    ]

    #list of sections that when combined form a valid body
    good_sections = [good_changelogs, good_impact, good_risklevel, template_checklist]

    #list of sections that when combined form the template
    template_sections = [template_changelogs, template_impact, template_risklevel, template_checklist]

    tests_passed = True
    #Test section not filled out
    #no need to test checklist section
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
    for i in range(len(good_sections)-1):
        test_sections = []
        for j in range(len(good_sections)):
            test_sections.append(good_sections[j].copy())
        for k in range(i+1,len(good_sections)):
            test_sections[i], test_sections[k] = test_sections[k],test_sections[i]
            tests_passed = run_test(f"Swapped sections: {i}, {k}", build_body(test_sections), False, DEBUG_MESSAGES) and tests_passed

    #Test missing section
    for i in range(len(good_sections)):
        test_sections = []
        for j in range(len(good_sections)):
            if i != j:
                test_sections.append(good_sections[j].copy())
        tests_passed = run_test(f"Missing Section: {i}", build_body(test_sections), False, DEBUG_MESSAGES) and tests_passed
    
    #Test good body:
    tests_passed = run_test("good documentation", build_body(good_sections), True, DEBUG_MESSAGES) and tests_passed

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
