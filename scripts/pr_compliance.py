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

    #3 nums not ok
    assert not title_is_ok("[HUDI-123] my fake pr")
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





def body_is_ok(body,PRINT_DEBUG=False):
    state = "CHANGELOGS"
    found = False
    def print_error(message):
        if PRINT_DEBUG:
            print(f"ERROR:(state: {state}, found: {found}, message: {message}")
    for line in body.splitlines():
        # ignore empty lines
        if len(line) == 0:
            continue

        if state == "CHANGELOGS":
            # see if it matches template
            if line == "### Change Logs":
                # we found it so now we need to make sure there is additional text
                if not found:
                    found = True
                else:
                    print_error("duplicate \"### Change Logs\" ")
                    return False
            elif line == "### Impact" or line.startswith("**Risk level:") or line == "### Contributor's checklist":
                print_error(f"Out of order section or missing description \"{line}\"")
                return False

            # make sure there is additional text before looking for impact
            elif found and line != "_Describe context and summary for this change. Highlight if any code was copied._":
                # this is the default so we want to see different text that is actually explaining things
                state = "IMPACT"
                found = False


        elif state == "IMPACT":
            # see if it matches template
            if line == "### Impact":
                # we found it so now we need to make sure there is additional text
                if not found:
                    found = True
                else:
                    print_error("duplicate \"### Impact\" ")
                    return False
            elif line == "### Change Logs" or line.startswith("**Risk level:") or line == "### Contributor's checklist":
                print_error(f"Out of order section or missing description\"{line}\"")
                return False
            # make sure there is additional text before looking for risk level
            elif found and line != "_Describe any public API or user-facing feature change or any performance impact._":
                # this is the default so we want to see different text that is actually explaining things
                state = "RISKLEVEL"
                found = False

        elif state == "RISKLEVEL":
            if line.startswith("**Risk level:"):
                #if we already saw this then they shouldn't have it again
                if found:
                    print_error("duplicate line starting with \"**Risk level:\" ")
                    return False
                #if it is this line then they never chose a risk level
                if line == "**Risk level: none | low | medium | high**":
                    print_error("risk level not chosen ")
                    return False

                # an explanation is not required for none or low
                if "NONE" in line.upper() or "LOW" in line.upper():
                    state = "CHECKLIST"
                    found = False
                elif "MEDIUM" in line.upper() or "HIGH" in line.upper():
                    # an explanation is required so we don't change state
                    found = True
                else:
                    #they put something weird in for risk level
                    print_error("invalid choice for risk level")
                    return False
            elif line == "### Impact" or line == "### Change Logs" or line == "### Contributor's checklist":
                print_error(f"Out of order section or missing description \"{line}\"")
                return False

            elif found and line != "_Choose one. If medium or high, explain what verification was done to mitigate the risks._":
                #explanation found so we can change states
                state = "CHECKLIST"
                found = False


        elif state == "CHECKLIST":
            if line.startswith("**Risk level:"):
                print_error("duplicate line starting with \"**Risk level:\" ")
                return False
            if line == "### Impact" or line == "### Change Logs" or line.startswith("**Risk level:"):
                print_error(f"Out of order section \"{line}\"")
                return False
            if line == "### Contributor's checklist":
                #this has passed the check
                return True
    #we didn't exit(0) so something is wrong with the template
    print_error("template is not filled out properly")
    return False

def test_body():
    debug_messages = True
    just_the_template = [
        "### Change Logs",
        "",
        "_Describe context and summary for this change. Highlight if any code was copied._",
        "",
        "### Impact",
        "",
        "_Describe any public API or user-facing feature change or any performance impact._",
        "",
        "**Risk level: none | low | medium | high**",
        "",
        "_Choose one. If medium or high, explain what verification was done to mitigate the risks._",
        "",
        "### Contributor's checklist",
        "",
        "- [ ] Read through [contributor's guide](https://hudi.apache.org/contribute/how-to-contribute)",
        "- [ ] Change Logs and Impact were stated clearly",
        "- [ ] Adequate tests were added if applicable",
        "- [ ] CI passed"
        ]

    print("just_the_template")
    assert not body_is_ok("\n".join(just_the_template),debug_messages)

    duplicate_change_logs = [
        "### Change Logs",
        "",
        "### Change Logs",
        "_Describe context and summary for this change. Highlight if any code was copied._",
        "",
        "### Impact",
        "",
        "_Describe any public API or user-facing feature change or any performance impact._",
        "",
        "**Risk level: none | low | medium | high**",
        "",
        "_Choose one. If medium or high, explain what verification was done to mitigate the risks._",
        "",
        "### Contributor's checklist",
        "",
        "- [ ] Read through [contributor's guide](https://hudi.apache.org/contribute/how-to-contribute)",
        "- [ ] Change Logs and Impact were stated clearly",
        "- [ ] Adequate tests were added if applicable",
        "- [ ] CI passed"
        ]

    print("duplicate_change_logs")
    assert not body_is_ok("\n".join(duplicate_change_logs),debug_messages)

    describe_context = [
        "### Change Logs",
        "",
        "describing context",
        "",
        "### Impact",
        "",
        "_Describe any public API or user-facing feature change or any performance impact._",
        "",
        "**Risk level: none | low | medium | high**",
        "",
        "_Choose one. If medium or high, explain what verification was done to mitigate the risks._",
        "",
        "### Contributor's checklist",
        "",
        "- [ ] Read through [contributor's guide](https://hudi.apache.org/contribute/how-to-contribute)",
        "- [ ] Change Logs and Impact were stated clearly",
        "- [ ] Adequate tests were added if applicable",
        "- [ ] CI passed"
        ]
    print("describe_context")
    assert not body_is_ok("\n".join(describe_context),debug_messages)

    describe_impact = [
        "### Change Logs",
        "",
        "_Describe context and summary for this change. Highlight if any code was copied._",
        "describing the context",
        "",
        "### Impact",
        "",
        "describing impact",
        "",
        "**Risk level: none | low | medium | high**",
        "",
        "_Choose one. If medium or high, explain what verification was done to mitigate the risks._",
        "",
        "### Contributor's checklist",
        "",
        "- [ ] Read through [contributor's guide](https://hudi.apache.org/contribute/how-to-contribute)",
        "- [ ] Change Logs and Impact were stated clearly",
        "- [ ] Adequate tests were added if applicable",
        "- [ ] CI passed"
        ]
    print("describe_impact")
    assert not body_is_ok("\n".join(describe_impact),debug_messages)

    duplicate_impact = [
        "### Change Logs",
        "",
        "_Describe context and summary for this change. Highlight if any code was copied._",
        "describing the context",
        "",
        "### Impact",
        "### Impact",
        "",
        "describing impact",
        "",
        "**Risk level: none | low | medium | high**",
        "",
        "_Choose one. If medium or high, explain what verification was done to mitigate the risks._",
        "",
        "### Contributor's checklist",
        "",
        "- [ ] Read through [contributor's guide](https://hudi.apache.org/contribute/how-to-contribute)",
        "- [ ] Change Logs and Impact were stated clearly",
        "- [ ] Adequate tests were added if applicable",
        "- [ ] CI passed"
        ]
    print("duplicate_impact")
    assert not body_is_ok("\n".join(duplicate_impact),debug_messages)

    choose_risk_level_none = [
        "### Change Logs",
        "",
        "_Describe context and summary for this change. Highlight if any code was copied._",
        "describing context",
        "",
        "### Impact",
        "describing impact",
        "",
        "_Describe any public API or user-facing feature change or any performance impact._",
        "",
        "**Risk level: none**",
        "",
        "_Choose one. If medium or high, explain what verification was done to mitigate the risks._",
        "",
        "### Contributor's checklist",
        "",
        "- [ ] Read through [contributor's guide](https://hudi.apache.org/contribute/how-to-contribute)",
        "- [ ] Change Logs and Impact were stated clearly",
        "- [ ] Adequate tests were added if applicable",
        "- [ ] CI passed"
        ]
    print("choose_risk_level_none")
    assert body_is_ok("\n".join(choose_risk_level_none),debug_messages)

    duplicate_risk_level = [
        "### Change Logs",
        "",
        "_Describe context and summary for this change. Highlight if any code was copied._",
        "describing context",
        "",
        "### Impact",
        "describing impact",
        "",
        "_Describe any public API or user-facing feature change or any performance impact._",
        "",
        "**Risk level: none**",
        "",
        "**Risk level: none | low | medium | high**",
        "_Choose one. If medium or high, explain what verification was done to mitigate the risks._",
        "",
        "### Contributor's checklist",
        "",
        "- [ ] Read through [contributor's guide](https://hudi.apache.org/contribute/how-to-contribute)",
        "- [ ] Change Logs and Impact were stated clearly",
        "- [ ] Adequate tests were added if applicable",
        "- [ ] CI passed"
        ]
    print("duplicate_risk_level")
    assert not body_is_ok("\n".join(duplicate_risk_level),debug_messages)


    choose_risk_level_high = [
        "### Change Logs",
        "",
        "_Describe context and summary for this change. Highlight if any code was copied._",
        "describing context",
        "",
        "### Impact",
        "",
        "describing impact",
        "_Describe any public API or user-facing feature change or any performance impact._",
        "",
        "**Risk level: high**",
        "",
         "_Choose one. If medium or high, explain what verification was done to mitigate the risks._",
        "",
        "### Contributor's checklist",
        "",
        "- [ ] Read through [contributor's guide](https://hudi.apache.org/contribute/how-to-contribute)",
        "- [ ] Change Logs and Impact were stated clearly",
        "- [ ] Adequate tests were added if applicable",
        "- [ ] CI passed"
        ]
    print("choose_risk_level_high")
    assert not body_is_ok("\n".join(choose_risk_level_high),debug_messages)

    no_risk_level = [
        "### Change Logs",
        "",
        "_Describe context and summary for this change. Highlight if any code was copied._",
        "describing context",
        "",
        "### Impact",
        "",
        "describing impact",
        "_Describe any public API or user-facing feature change or any performance impact._",
        "",
        "**Risk level: **",
        "",
         "_Choose one. If medium or high, explain what verification was done to mitigate the risks._",
        "",
        "### Contributor's checklist",
        "",
        "- [ ] Read through [contributor's guide](https://hudi.apache.org/contribute/how-to-contribute)",
        "- [ ] Change Logs and Impact were stated clearly",
        "- [ ] Adequate tests were added if applicable",
        "- [ ] CI passed"
        ]
    print("no_risk_level")
    assert not body_is_ok("\n".join(no_risk_level),debug_messages)

    weird_risk_level = [
        "### Change Logs",
        "",
        "_Describe context and summary for this change. Highlight if any code was copied._",
        "describing context",
        "",
        "### Impact",
        "",
        "describing impact",
        "_Describe any public API or user-facing feature change or any performance impact._",
        "",
        "**Risk level: onehouse**",
        "",
         "_Choose one. If medium or high, explain what verification was done to mitigate the risks._",
        "",
        "### Contributor's checklist",
        "",
        "- [ ] Read through [contributor's guide](https://hudi.apache.org/contribute/how-to-contribute)",
        "- [ ] Change Logs and Impact were stated clearly",
        "- [ ] Adequate tests were added if applicable",
        "- [ ] CI passed"
        ]
    print("weird_risk_level")
    assert not body_is_ok("\n".join(weird_risk_level),debug_messages)

    choose_risk_level_high_explained = [
        "### Change Logs",
        "",
        "_Describe context and summary for this change. Highlight if any code was copied._",
        "describing context",
        "",
        "### Impact",
        "",
        "describing impact",
        "_Describe any public API or user-facing feature change or any performance impact._",
        "",
        "**Risk level: high**",
        "",
         "_Choose one. If medium or high, explain what verification was done to mitigate the risks._",
         "explaining my impact",
        "",
        "### Contributor's checklist",
        "",
        "- [ ] Read through [contributor's guide](https://hudi.apache.org/contribute/how-to-contribute)",
        "- [ ] Change Logs and Impact were stated clearly",
        "- [ ] Adequate tests were added if applicable",
        "- [ ] CI passed"
        ]
    print("choose_risk_level_high_explained")
    assert body_is_ok("\n".join(choose_risk_level_high_explained),debug_messages)

    good_documentation = [
        "### Change Logs",
        "",
        "_Describe context and summary for this change. Highlight if any code was copied._",
        "actually describe context",
        "",
        "### Impact",
        "",
        "actually describe impact",
        "_Describe any public API or user-facing feature change or any performance impact._",
        "",
        "**Risk level: medium **",
        "",
        "_Choose one. If medium or high, explain what verification was done to mitigate the risks._",
        "",
        "Risk is medium so we need to describe it",
        "### Contributor's checklist",
        "",
        "- [ ] Read through [contributor's guide](https://hudi.apache.org/contribute/how-to-contribute)",
        "- [ ] Change Logs and Impact were stated clearly",
        "- [ ] Adequate tests were added if applicable",
        "- [ ] CI passed"
        ]
    print("good_documentation")
    assert body_is_ok("\n".join(good_documentation),debug_messages)

    good_documentation_no_describe_risk = [
        "### Change Logs",
        "",
        "_Describe context and summary for this change. Highlight if any code was copied._",
        "actually describe context",
        "",
        "### Impact",
        "",
        "actually describe impact",
        "_Describe any public API or user-facing feature change or any performance impact._",
        "",
        "**Risk level: low **",
        "",
        "_Choose one. If medium or high, explain what verification was done to mitigate the risks._",
        "",
        "### Contributor's checklist",
        "",
        "- [ ] Read through [contributor's guide](https://hudi.apache.org/contribute/how-to-contribute)",
        "- [ ] Change Logs and Impact were stated clearly",
        "- [ ] Adequate tests were added if applicable",
        "- [ ] CI passed"
        ]
    print("good_documentation_no_describe_risk")
    assert body_is_ok("\n".join(good_documentation_no_describe_risk),debug_messages)

    no_checklist = [
        "### Change Logs",
        "",
        "_Describe context and summary for this change. Highlight if any code was copied._",
        "actually describe context",
        "",
        "### Impact",
        "",
        "actually describe impact",
        "_Describe any public API or user-facing feature change or any performance impact._",
        "",
        "**Risk level: medium **",
        "",
        "_Choose one. If medium or high, explain what verification was done to mitigate the risks._",
        "",
        "Risk is medium so we need to describe it",
        ]
    print("no_checklist")
    assert not body_is_ok("\n".join(no_checklist),debug_messages)

if __name__ == '__main__':
    title = os.getenv("REQUEST_TITLE")
    body = os.getenv("REQUEST_BODY")
    if not title_is_ok(title):
        exit(-1)
    if not body_is_ok(body):
        exit(-2)
    exit(0)


