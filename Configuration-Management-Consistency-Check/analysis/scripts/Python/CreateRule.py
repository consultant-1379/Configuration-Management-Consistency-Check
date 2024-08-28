def nav_to_page():
    """
    navigates to the Edit rule page
    """
    for page in Document.Pages:
          if (page.Title == "Create Rule"):
              Document.ActivePageReference=page


def main():
    """
    main flow
    """
    Document.Properties['moClassNameError'] ="Select One MOClass"
    Document.Properties['AttributeNameError'] ="Select One Attribute"
    Document.Properties['FilterTrigger']='Create'
main()
nav_to_page()
