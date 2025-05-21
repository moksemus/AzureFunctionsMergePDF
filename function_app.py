import azure.functions as func
import logging
import json
import base64
import fitz  # PyMuPDF
from io import BytesIO
import traceback
from PyPDF2 import PdfReader, PdfWriter
from PyPDF2.generic import TextStringObject
from PyPDF2.generic import DictionaryObject, NameObject, BooleanObject, ArrayObject
from collections import defaultdict


app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)



@app.route(route="detect_pdf_text_layer")
def detect_pdf_text_layer(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_json = req.get_json()
        pdf_bytes = base64_to_pdf(req_json['file_content'].get('$content'))
        
        text_layer, all_pages_text_layer = pdf_text_layer_info(pdf_bytes)
        
        response_data = {
            "text_layer": text_layer,
            "all_pages_text_layer": all_pages_text_layer
        }
        
        return func.HttpResponse(
            body=json.dumps(response_data),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        return func.HttpResponse(str(e), status_code=400)

def base64_to_pdf(base64_string):
    file_bytes = base64.b64decode(base64_string)
    if file_bytes[0:4] != b"%PDF":
        raise ValueError("Missing the PDF file signature")
    return file_bytes

def pdf_text_layer_info(pdf_bytes: bytes) -> tuple:
    """
    Check if any page in the PDF contains a text layer and if all pages have a text layer.
    Returns (text_layer_found, all_pages_have_text_layer).
    """
    pdf_document = fitz.open(stream=pdf_bytes, filetype="pdf")
    any_text_layer = False
    all_text_layer = True
    
    for page_num in range(len(pdf_document)):
        page = pdf_document.load_page(page_num)
        text = page.get_text()
        if text.strip():
            any_text_layer = True
        else:
            all_text_layer = False
    
    return any_text_layer, all_text_layer






@app.route(route="merge_pdf_pypdf2")
def merge_pdf_pypdf2(req: func.HttpRequest) -> func.HttpResponse:
    try:
        pdf_base64_strings_list = [item.get('$content') for item in req.get_json()['file_content']]
        merged_pdf_base64_string = merge_pdfs(pdf_base64_strings_list)

        return func.HttpResponse(
            body=json.dumps({
                "$content-type": "application/pdf",
                "$content": merged_pdf_base64_string
            }),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)

def base64_to_pdf(base64_string):
    file_bytes = base64.b64decode(base64_string)
    if file_bytes[0:4] != b"%PDF":
        raise ValueError("Missing the PDF file signature")
    return BytesIO(file_bytes)

def merge_pdfs(pdf_base64_list):
    """
    Merge multiple PDFs (with form fields) and preserve all fields:
      - text fields
      - checkboxes
      - single-select (radio) buttons
    """
    writer = PdfWriter()

    # Collect fields from each PDF
    all_fields = ArrayObject()  # Will accumulate references to form fields
    found_form = False

    for pdf_b64 in pdf_base64_list:
        pdf_stream = base64_to_pdf(pdf_b64)
        # Check if the object is a BytesIO stream
        if not isinstance(pdf_stream, BytesIO):
            pdf_stream = BytesIO(pdf_stream)
        pdf_stream.seek(0)
        reader = PdfReader(pdf_stream)

        # 1) Append all pages to the writer
        for page in reader.pages:
            writer.add_page(page)

        # 2) If the PDF has an AcroForm, gather its fields
        root = reader.trailer["/Root"]
        if "/AcroForm" in root:
            found_form = True
            acroform = root["/AcroForm"]

            if "/Fields" in acroform:
                for f in acroform["/Fields"]:
                    # f is (usually) an IndirectObject reference to a field dict
                    all_fields.append(f)

    # 3) Build a single AcroForm in the final PDF if any fields were found
    if found_form and len(all_fields) > 0:
        final_acroform = DictionaryObject()
        final_acroform[NameObject("/Fields")] = all_fields
        final_acroform[NameObject("/NeedAppearances")] = BooleanObject(True)
        
        # Optionally copy default appearance or other keys from the last PDF’s AcroForm
        # e.g. if you want to preserve fonts / default appearances from the last PDF:
        # if "/DA" in acroform:
        #     final_acroform[NameObject("/DA")] = acroform["/DA"]

        writer._root_object[NameObject("/AcroForm")] = final_acroform

    # 4) Write out the merged PDF
    merged_pdf_io = BytesIO()
    writer.write(merged_pdf_io)
    merged_pdf_io.seek(0)

    return base64.b64encode(merged_pdf_io.read()).decode("utf-8")




@app.route(route="merge_pdf_fitz")
def merge_pdf_fitz(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # Form an array of base64 strings of the pdfs to merge from the $content parameters & call the merge_pdfs function on that array
        pdf_base64_strings_list = [item.get('$content') for item in req.get_json()['file_content']]
        merged_pdf_base64_string = merge_pdfs(pdf_base64_strings_list)
        
        ###HTTP response for MERGE operation###
        # Return the merged pdf base64 string in a Power Automate content object in an HTTP response
        return func.HttpResponse(
        body=json.dumps({
            "$content-type": "application/pdf",
            "$content": merged_pdf_base64_string
            }),
        mimetype="application/json",
        status_code=200
        )              
    
    except Exception as e:
        # If there is an error, log the error & then return the error message in an HTTP response
        debug_error = logging.exception(f"An error occurred: {str(e)}\n\nTraceback:\n{traceback.format_exc()}")
        return func.HttpResponse(
            f"Error: {str(e)}\n\nTraceback:\n{traceback.format_exc()}",
            status_code=500
        )

# Function used in each MERGE & SPLIT operation to get the PDF file bytes
def base64_to_pdf(base64_string):
    file_bytes = base64.b64decode(base64_string, validate=True)
    if file_bytes[0:4] != b"%PDF":
        raise ValueError("Missing the PDF file signature")
    return file_bytes

def merge_pdfs(pdf_base64_list):
    result = fitz.open()
    
    # First step: collect all form field values from all PDFs
    all_form_values = {}
    
    for pdf_base64 in pdf_base64_list:
        pdf_bytes = base64_to_pdf(pdf_base64)
        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            # Get form field values using the correct method name
            field_data = {}
            for page in doc:
                for widget in page.widgets():
                    if widget.field_name:
                        # Only store if this has a value
                        if widget.field_type == fitz.PDF_WIDGET_TYPE_RADIOBUTTON:
                            # For radio buttons, we need to check if it's selected
                            if hasattr(widget, 'field_flags') and (widget.field_flags & 2**15):
                                field_data[widget.field_name] = widget.field_value
                        else:
                            field_data[widget.field_name] = widget.field_value
            
            # Add to our collective field values
            all_form_values.update(field_data)
    
    # Second step: merge the PDFs with annotations preserved
    for pdf_base64 in pdf_base64_list:
        pdf_bytes = base64.b64decode(pdf_base64)
        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            # Use the same technique that works in your split function
            result.insert_pdf(doc, annots=True)
    
    # Final step: apply collected values to ensure consistent field values
    # Wait a moment before updating fields (sometimes helps with stability)
    processed_fields = set()
    
    for page in result:
        for widget in page.widgets():
            field_name = widget.field_name
            if field_name in all_form_values and field_name not in processed_fields:
                try:
                    if widget.field_type == fitz.PDF_WIDGET_TYPE_RADIOBUTTON:
                        # For radio buttons, handle the entire group together
                        radio_group_name = field_name
                        
                        # Skip if we've already processed this group
                        if radio_group_name in processed_fields:
                            continue
                        
                        # Get the target value to select
                        target_value = all_form_values.get(radio_group_name)
                        if not target_value:
                            continue  # Skip if no value to set
                        
                        try:
                            # Try using the document-level API to set field values
                            # This works with radio buttons as groups rather than individual widgets
                            result.set_field_value(radio_group_name, target_value)
                        except AttributeError:
                            continue

                        processed_fields.add(radio_group_name)
    
                    else:
                        # For other field types
                        widget.field_value = all_form_values[field_name]
                        widget.update()
                    
                    processed_fields.add(field_name)
                    
                except Exception as e:
                    pass  # Skip if there's an issue
    
    # Save with settings that are compatible with your PyMuPDF version
    buffer = BytesIO()
    result.save(
        buffer, 
        garbage=0,  # No garbage collection to avoid removing form elements
        deflate=True, 
        clean=False  # Don't clean/remove any elements
    )
    buffer.seek(0)
    merged_pdf_bytes = buffer.read()
    
    return base64.b64encode(merged_pdf_bytes).decode("utf-8")
       

         
            




@app.route(route="split_pdf_pypdf2")
def split_pdf_pypdf2(req: func.HttpRequest) -> func.HttpResponse: 
    try:
        req_json = req.get_json()
        pdf_bytes = base64_to_pdf(req_json['file_content'].get('$content'))
        page_numbers = req_json.get('pages')
        split_text = req_json.get('split_text')
        
        if page_numbers:
            split_base64_strings = split_pdf_by_page_numbers(pdf_bytes, page_numbers)
        elif split_text:
            # Determine if PDF has a text layer
            if pdf_has_text_layer(pdf_bytes):
                split_base64_strings = split_pdf_by_text(pdf_bytes, split_text)  # Use text extraction
            else:
                #split_base64_strings = split_pdf_by_ocr_text(pdf_bytes, split_text)  # Use OCR extraction
                return func.HttpResponse("Method 'TEXT' does not work on pdfs that do not have text-layers. Use a different method or only use on pdfs with text-layers.", status_code=400)

        else:
            return func.HttpResponse("Invalid. Must provide a 'pages' array to split by page or a 'split_text' to split by exact text.", status_code=400)

        response_data = [{"$content-type": "application/pdf", "$content": pdf} for pdf in split_base64_strings]

        return func.HttpResponse(
            body=json.dumps(response_data),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        return func.HttpResponse(str(e), status_code=400)

def base64_to_pdf(base64_string):
    file_bytes = base64.b64decode(base64_string)
    if file_bytes[0:4] != b"%PDF":
        raise ValueError("Missing the PDF file signature")
    return file_bytes

def pdf_has_text_layer(pdf_bytes: bytes) -> bool:
    """
    Check if the PDF contains a text layer.
    Returns True if text is found, False otherwise.
    """
    pdf_document = fitz.open(stream=pdf_bytes, filetype="pdf")
    for page_num in range(len(pdf_document)):
        page = pdf_document.load_page(page_num)
        text = page.get_text()
        if text.strip():
            return True
    return False

def split_pdf_by_page_numbers(pdf_bytes, page_numbers):
    logging.info("Function is using split_pdf_by_page_numbers")
    pdf_reader = PdfReader(BytesIO(pdf_bytes))
    total_pages = len(pdf_reader.pages)
    result_base64_strings = []

    # Ensure the split list includes the first page
    if page_numbers[0] != 1:
        page_numbers = [1] + page_numbers

    # Get the original form field values if they exist
    original_field_values = {}
    if pdf_reader.get_fields():
        for field_name, field in pdf_reader.get_fields().items():
            if "/V" in field:
                original_field_values[field_name] = field["/V"]

    for i in range(len(page_numbers)):
        start_page = page_numbers[i] - 1
        end_page = page_numbers[i + 1] - 2 if i < len(page_numbers) - 1 else total_pages - 1

        pdf_writer = PdfWriter()
        
        # Track all field annotations in this section
        section_fields = {}
        
        # Add pages to the new PDF and collect field annotations
        for j in range(start_page, end_page + 1):
            page = pdf_reader.pages[j]
            pdf_writer.add_page(page)
            
            # Check if the page has form field annotations
            if "/Annots" in page:
                page_annots = page["/Annots"]
                for annot_idx, annot_ref in enumerate(page_annots):
                    annot = annot_ref.get_object()
                    if annot.get("/Subtype") == "/Widget":
                        # Handle both direct fields and fields with parent references
                        if "/Parent" in annot:
                            parent = annot["/Parent"].get_object()
                            if "/T" in parent:
                                field_name = parent["/T"]
                                if isinstance(field_name, TextStringObject):
                                    section_fields[str(field_name)] = annot
                        elif "/T" in annot:
                            field_name = annot["/T"]
                            if isinstance(field_name, TextStringObject):
                                section_fields[str(field_name)] = annot

        # If we found fields, rebuild the AcroForm properly
        if section_fields:
            # Create a new AcroForm dictionary
            acro_form = DictionaryObject()
            
            # Standard settings for most interactive forms
            acro_form[NameObject("/NeedAppearances")] = BooleanObject(True)
            
            # Add standard form resources if they exist in the original document
            if pdf_reader.root_object.get("/AcroForm") is not None:
                original_acro_form = pdf_reader.root_object["/AcroForm"]
                
                # Copy essential form resources
                for key in ["/DR", "/DA", "/Q", "/XFA"]:
                    if key in original_acro_form:
                        acro_form[NameObject(key)] = original_acro_form[key]
            
            # Create the Fields array for the new AcroForm
            field_array = ArrayObject()
            for field_name, field_ref in section_fields.items():
                field_array.append(field_ref)
            
            acro_form[NameObject("/Fields")] = field_array
            pdf_writer._root_object[NameObject("/AcroForm")] = acro_form
            
            # Restore field values from the original document
            for field_name, field_value in original_field_values.items():
                if field_name in section_fields:
                    try:
                        # Use two approaches for updating field values
                        # 1. Direct dictionary update
                        field_obj = section_fields[field_name]
                        if isinstance(field_value, TextStringObject):
                            field_obj[NameObject("/V")] = field_value
                        
                        # 2. Use PyPDF2's built-in update mechanism
                        update_dict = {field_name: field_value}
                        pdf_writer.update_page_form_field_values(pdf_writer.pages[0], update_dict)
                    except Exception as e:
                        logging.warning(f"Could not update field value for {field_name}: {str(e)}")

        # Write to memory and return as base64
        output_stream = BytesIO()
        pdf_writer.write(output_stream)
        output_stream.seek(0)
        split_pdf_bytes = output_stream.read()
        result_base64_strings.append(base64.b64encode(split_pdf_bytes).decode("utf-8"))

    return result_base64_strings


def split_pdf_by_text(pdf_bytes, split_text):
    logging.info("Function is using split_pdf_by_text")
    pdf_reader = PdfReader(BytesIO(pdf_bytes))
    result = []
    current_range = []
    base64_results = []

    # Identify page ranges based on text occurrence
    for page_num, page in enumerate(pdf_reader.pages):
        page_text = page.extract_text()
        if split_text in page_text:
            if current_range:
                result.append(current_range)
                current_range = []
        current_range.append(page_num)
    if current_range:
        result.append(current_range)

    # Get the original form field values if they exist
    original_field_values = {}
    if pdf_reader.get_fields():
        for field_name, field in pdf_reader.get_fields().items():
            if "/V" in field:
                original_field_values[field_name] = field["/V"]

    # Process each identified page range as a separate PDF
    for page_range in result:
        pdf_writer = PdfWriter()
        
        # Track all field annotations in this section
        section_fields = {}
        
        # Add pages to the new PDF and collect field annotations
        for page_idx in page_range:
            page = pdf_reader.pages[page_idx]
            pdf_writer.add_page(page)
            
            # Check if the page has form field annotations
            if "/Annots" in page:
                page_annots = page["/Annots"]
                for annot_idx, annot_ref in enumerate(page_annots):
                    annot = annot_ref.get_object()
                    if annot.get("/Subtype") == "/Widget":
                        # Handle both direct fields and fields with parent references
                        if "/Parent" in annot:
                            parent = annot["/Parent"].get_object()
                            if "/T" in parent:
                                field_name = parent["/T"]
                                if isinstance(field_name, TextStringObject):
                                    section_fields[str(field_name)] = annot
                        elif "/T" in annot:
                            field_name = annot["/T"]
                            if isinstance(field_name, TextStringObject):
                                section_fields[str(field_name)] = annot

        # If we found fields, rebuild the AcroForm properly
        if section_fields:
            # Create a new AcroForm dictionary
            acro_form = DictionaryObject()
            
            # Standard settings for most interactive forms
            acro_form[NameObject("/NeedAppearances")] = BooleanObject(True)
            
            # Add standard form resources if they exist in the original document
            if pdf_reader.root_object.get("/AcroForm") is not None:
                original_acro_form = pdf_reader.root_object["/AcroForm"]
                
                # Copy essential form resources
                for key in ["/DR", "/DA", "/Q", "/XFA"]:
                    if key in original_acro_form:
                        acro_form[NameObject(key)] = original_acro_form[key]
            
            # Create the Fields array for the new AcroForm
            field_array = ArrayObject()
            for field_name, field_ref in section_fields.items():
                field_array.append(field_ref)
            
            acro_form[NameObject("/Fields")] = field_array
            pdf_writer._root_object[NameObject("/AcroForm")] = acro_form
            
            # Restore field values from the original document
            for field_name, field_value in original_field_values.items():
                if field_name in section_fields:
                    try:
                        # Use two approaches for updating field values
                        # 1. Direct dictionary update
                        field_obj = section_fields[field_name]
                        if isinstance(field_value, TextStringObject):
                            field_obj[NameObject("/V")] = field_value
                        
                        # 2. Use PyPDF2's built-in update mechanism
                        update_dict = {field_name: field_value}
                        pdf_writer.update_page_form_field_values(pdf_writer.pages[0], update_dict)
                    except Exception as e:
                        logging.warning(f"Could not update field value for {field_name}: {str(e)}")

        # Write to memory and return as base64
        output_stream = BytesIO()
        pdf_writer.write(output_stream)
        output_stream.seek(0)
        split_pdf_bytes = output_stream.read()
        base64_results.append(base64.b64encode(split_pdf_bytes).decode("utf-8"))

    return base64_results










@app.route(route="split_pdf_fitz")
def split_pdf_fitz(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_json = req.get_json()
        pdf_bytes = base64_to_pdf(req_json['file_content'].get('$content'))
        page_numbers = req_json.get('pages')
        split_text = req_json.get('split_text')
        split_regex = req_json.get('split_regex')
        
        if page_numbers:
            split_base64_strings = split_pdf_by_page_numbers(pdf_bytes, page_numbers)
        elif split_text or split_regex:
            # Determine if PDF has a text layer
            if pdf_has_text_layer(pdf_bytes):
                split_base64_strings = split_pdf_by_text(pdf_bytes, split_text, split_regex)
            else:
                return func.HttpResponse("Text & regex methods do not work on PDFs without text layers. Use a different method or only use on PDFs with text layers.", status_code=400)
        else:
            return func.HttpResponse("Invalid. Must provide a 'pages' array to split by page, a 'split_text' to split by exact text, or a 'split_regex to split by text matching a regex expression.", status_code=400)

        response_data = [{"$content-type": "application/pdf", "$content": pdf} for pdf in split_base64_strings]

        return func.HttpResponse(
            body=json.dumps(response_data),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        logging.exception(f"An error occurred: {str(e)}\n\nTraceback:\n{traceback.format_exc()}")
        return func.HttpResponse(
            f"Error: {str(e)}\n\nTraceback:\n{traceback.format_exc()}",
            status_code=500
        )

def base64_to_pdf(base64_string):
    file_bytes = base64.b64decode(base64_string, validate=True)
    if file_bytes[0:4] != b"%PDF":
        raise ValueError("Missing the PDF file signature")
    return file_bytes

def pdf_has_text_layer(pdf_bytes: bytes) -> bool:
    """
    Check if the PDF contains a text layer.
    Returns True if text is found, False otherwise.
    """
    pdf_document = fitz.open(stream=pdf_bytes, filetype="pdf")
    has_text = False
    
    for page_num in range(min(len(pdf_document), 3)):  # Check first 3 pages for efficiency
        page = pdf_document[page_num]
        text = page.get_text()
        if text.strip():
            has_text = True
            break
            
    pdf_document.close()
    return has_text

def pdf_has_form_fields(pdf_bytes: bytes) -> bool:
    """
    Check if the PDF contains any form fields throughout the entire document.
    Returns True if any form fields are found, False otherwise.
    """
    pdf_document = fitz.open(stream=pdf_bytes, filetype="pdf")
    has_fields = False
    
    # Check all pages for form fields
    for page_num in range(len(pdf_document)):
        page = pdf_document[page_num]
        try:
            widgets = page.widgets()
            if len(widgets) > 0:
                has_fields = True
                break
        except Exception as e:
            logging.warning(f"Error checking for widgets on page {page_num}: {str(e)}")
    
    # Also check for AcroForm in the PDF catalog
    try:
        if "AcroForm" in pdf_document.get_pdf_catalog():
            has_fields = True
    except Exception as e:
        logging.warning(f"Error checking for AcroForm: {str(e)}")
    
    pdf_document.close()
    return has_fields

def get_form_fields_info(pdf_bytes):
    """
    Extract comprehensive form field information including all metadata.
    This function creates a more detailed mapping of fields to ensure proper preservation.
    """
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    
    # Map fields to pages
    page_to_fields = defaultdict(set)
    field_to_pages = defaultdict(set)
    
    # Extract all field values and properties
    field_data = {}
    
    # Process each page
    for page_num in range(len(doc)):
        page = doc[page_num]
        
        # Get widgets from the page
        try:
            for widget in page.widgets():
                try:
                    field_name = widget.field_name
                    if field_name:
                        # Map field to page
                        page_to_fields[page_num].add(field_name)
                        field_to_pages[field_name].add(page_num)
                        
                        # Store complete field data
                        if field_name not in field_data:
                            field_data[field_name] = {
                                'value': widget.field_value if hasattr(widget, 'field_value') else None,
                                'type': widget.field_type if hasattr(widget, 'field_type') else None,
                                'flags': widget.field_flags if hasattr(widget, 'field_flags') else None,
                                'rect': widget.rect if hasattr(widget, 'rect') else None,
                                'appearance': None,  # Will be populated if needed
                            }
                except Exception as e:
                    logging.warning(f"Error processing widget on page {page_num}: {str(e)}")
        except Exception as e:
            logging.warning(f"Error accessing widgets on page {page_num}: {str(e)}")
    
    doc.close()
    return page_to_fields, field_to_pages, field_data

def process_split_document(pdf_bytes, start_page, end_page):
    """
    Creates a new document from the specified page range and ensures form fields are preserved.
    Using a different approach to ensure consistent form field preservation across all splits.
    """
    # Open the source document
    source_doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    
    # Create a new document for the page range
    new_doc = fitz.open()
    
    # Insert pages with complete annotations (crucial for form fields)
    new_doc.insert_pdf(source_doc, from_page=start_page, to_page=end_page, annots=True)
    
    # Explicitly handle XFA forms if present
    try:
        if hasattr(source_doc, "xref_xml_metadata") and source_doc.xref_xml_metadata > 0:
            # Copy XFA form data if available
            if hasattr(new_doc, "set_xml_metadata") and hasattr(source_doc, "xml_metadata"):
                new_doc.set_xml_metadata(source_doc.xml_metadata)
    except Exception as e:
        logging.warning(f"Error handling XFA forms: {str(e)}")
    
    # Ensure AcroForm is preserved
    try:
        if "AcroForm" in source_doc.get_pdf_catalog():
            # If there's an AcroForm in the source, make sure we're preserving all form-related elements
            logging.info("AcroForm found in source document, ensuring preservation")
    except Exception as e:
        logging.warning(f"Error checking for AcroForm: {str(e)}")
    
    # Save the document with careful settings to preserve all form functionality
    try:
        buffer = BytesIO()
        # Use specific PDF settings that maximize form preservation
        new_doc.save(
            buffer, 
            garbage=0,  # No garbage collection to avoid removing form elements
            deflate=True, 
            clean=False,  # Don't clean/remove any elements
            encryption=False,
            permissions=int(
                fitz.PDF_PERM_ACCESSIBILITY |
                fitz.PDF_PERM_PRINT |
                fitz.PDF_PERM_COPY |
                fitz.PDF_PERM_ANNOTATE
            ),
            preserve_annots=True,  # IMPORTANT: Ensure annotations are preserved
            embedded_files=True    # Keep embedded files if any
        )
        buffer.seek(0)
        pdf_bytes = buffer.read()
    except Exception as e:
        logging.warning(f"Error saving with options: {str(e)}. Using fallback method.")
        try:
            # Fallback with different options
            buffer = BytesIO()
            new_doc.save(buffer, garbage=0, clean=False)
            buffer.seek(0)
            pdf_bytes = buffer.read()
        except Exception as e2:
            logging.warning(f"Fallback method also failed: {str(e2)}. Using tobytes.")
            pdf_bytes = new_doc.tobytes()
    
    # Close both documents to free resources
    new_doc.close()
    source_doc.close()
    
    return base64.b64encode(pdf_bytes).decode("utf-8")

def split_pdf_by_page_numbers(pdf_bytes, page_numbers):
    """
    Split PDF by page numbers, preserving form fields and their values.
    Returns a list of base64-encoded PDF documents.
    """
    # Check if the PDF has form fields (checking ALL pages)
    has_form_fields = pdf_has_form_fields(pdf_bytes)
    if has_form_fields:
        logging.info("PDF contains form fields - using form-preserving splitting")
    
    # Open the document to get total page count
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    total_pages = len(doc)
    doc.close()
    
    result_base64_strings = []
    
    # Ensure the page_numbers list starts with 1
    if not page_numbers or page_numbers[0] != 1:
        page_numbers = [1] + (page_numbers if page_numbers else [])
    
    # Add the total number of pages + 1 if the last split point is not the end
    # This ensures we include the last page in our calculations
    if page_numbers[-1] <= total_pages:
        page_numbers.append(total_pages + 1)
    
    # Process each page range
    for i in range(len(page_numbers) - 1):
        start_page = page_numbers[i] - 1  # Convert to 0-based index
        end_page = page_numbers[i+1] - 2  # Convert to 0-based index
        
        # For the last range, ensure we include the final page
        if i == len(page_numbers) - 2:
            end_page = total_pages - 1  # Make sure to include the last page
        
        # Skip invalid ranges
        if start_page > end_page or start_page < 0 or end_page >= total_pages:
            continue
        
        # Process the document for this page range
        base64_pdf = process_split_document(pdf_bytes, start_page, end_page)
        result_base64_strings.append(base64_pdf)
    
    return result_base64_strings

def split_pdf_by_text(pdf_bytes, split_text=None, split_regex=None):
    """
    Split PDF by exact text occurrence or regex match, preserving form fields and their values.
    Returns a list of base64-encoded PDF documents.
    """
    has_form_fields = pdf_has_form_fields(pdf_bytes)
    if has_form_fields:
        logging.info("PDF contains form fields - using form-preserving splitting")
    
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    total_pages = len(doc)
    split_pages = [0]  # Start with page 0
    
    for page_num in range(total_pages):
        page = doc[page_num]
        text = page.get_text()
        
        if split_regex:
            if re.search(split_regex, text) and page_num > 0:
                split_pages.append(page_num)
        elif split_text:
            if split_text in text and page_num > 0:
                split_pages.append(page_num)
    
    if split_pages[-1] != total_pages - 1:
        split_pages.append(total_pages)
    else:
        split_pages.append(total_pages)
    
    doc.close()
    
    result_base64_strings = []
    for i in range(len(split_pages) - 1):
        start_page = split_pages[i]
        end_page = split_pages[i + 1] - 1
        
        if end_page < start_page:
            continue
            
        base64_pdf = process_split_document(pdf_bytes, start_page, end_page)
        result_base64_strings.append(base64_pdf)
    
    return result_base64_strings