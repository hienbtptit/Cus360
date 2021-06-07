*** Settings ***
Library    SeleniumLibrary
Library    Collections
Library     String
Resource    ../Data/InputData.robot
*** Keywords ***
Begin web test
    open browser    about:blank    ${BROWSER}
    maximize browser window

Begin web test with headlessmode
    ${chrome_options} =     Evaluate    sys.modules['selenium.webdriver'].ChromeOptions()    sys, selenium.webdriver
    Call Method    ${chrome_options}   add_argument    headless
    Call Method    ${chrome_options}   add_argument    disable-gpu
    ${options}=     Call Method     ${chrome_options}    to_capabilities
     Open Browser    about:blank    browser=chrome     desired_capabilities=${options}
     Maximize Browser Window

End web test
    close browser

Navigate to
    [Arguments]    ${url}
    go to     ${url}

Get all element has same attribute
    [Arguments]    ${attribute}
    ${list_element} =    Get WebElements    ${attribute}
    [Return]    ${list_element}

Get web element id from atribute
    [Arguments]    ${list_web_elements}    ${attribute}
    ${list}    create list
    FOR    ${element}    IN    @{list_web_elements}
            Log    ${element}
            Append To List   ${list}    ${element.get_attribute('${attribute}')}
    END
    [Return]    @{list}

Go Back
    Execute Javascript  history.back()