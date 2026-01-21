"""Self-contained standalone replica of `submit_contact_form_old`.

No imports from the project; suitable to run independently.
- Tries Selenium Chrome if available, otherwise falls back to HTTP POST via `requests`.
- Updates `contact_urls` row using `psycopg2` when DB credentials are provided via env vars.

Usage example:
    from submit_contact_form_old_impl import submit_contact_form_old
    result = submit_contact_form_old(form_data, generated_message)

Environment variables used for DB (optional):
- DATABASE_URL or DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
"""

from datetime import timezone
try:
    import pytz
    PYTZ_AVAILABLE = True
except Exception:
    PYTZ_AVAILABLE = False
try:
    from dateutil import parser as dateutil_parser
    DATEUTIL_AVAILABLE = True
except Exception:
    DATEUTIL_AVAILABLE = False
import logging
import os
import tempfile
import uuid
import time
import random
from datetime import datetime
from typing import Dict, Any, Optional
import json
import uuid

WORKER_ID = str(uuid.uuid4())
LOCK_TIMEOUT_MINUTES = 15
MAX_RETRIES = 3
import requests
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import urljoin, urlparse
try:
    import lxml.html as lh
    LXML_AVAILABLE = True
except Exception:
    LXML_AVAILABLE = False

# Optional selenium usage
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import Select
    from selenium.webdriver.chrome.options import Options
    SELENIUM_AVAILABLE = True
except Exception:
    SELENIUM_AVAILABLE = False

# Optional DB (psycopg2)
try:
    import psycopg2
    PSYCOPG2_AVAILABLE = True
except Exception:
    PSYCOPG2_AVAILABLE = False

logger = logging.getLogger("submit_contact_form_old_impl")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

API_KEY_2CAPTCHA = os.getenv('API_KEY_2CAPTCHA', None)

FIELD_KEYWORDS = {
    "name": ["name", "full name", "fullname", "your-name", "contact-name", "first", "last", "first-name", "last-name"],
    "email": ["email", "e-mail", "mail"],
    "subject": ["subject", "topic", "reason"],
    "message": ["message", "comment", "comments", "enquiry", "inquiry", "description", "body","describe"],
    "phone": ["phone", "tel", "mobile", "contact-number"],
    "company": ["company", "organization", "organisation", "org"],
}

SUBMIT_TEXT_KEYWORDS = ["send", "submit", "contact", "enquire", "apply", "message"]


# --- Helpers copied from original ---

def text_of_label_for(driver, input_elem):
    try:
        id_attr = input_elem.get_attribute("id")
        if id_attr:
            labels = driver.find_elements(By.XPATH, f"//label[@for='{id_attr}']")
            if labels:
                return " ".join([l.text for l in labels]).strip()
        parent = input_elem.find_element(By.XPATH, "ancestor::label[1]")
        if parent:
            return parent.text.strip()
    except Exception:
        return ""
    return ""


def attr_texts(elem):
    parts = []
    for a in ("name", "id", "placeholder", "aria-label", "title", "class"):
        try:
            v = elem.get_attribute(a)
            if v:
                parts.append(v)
        except Exception:
            pass
    return " ".join(parts).lower()


def matches_keywords(text, keywords):
    if not text:
        return False
    text = text.lower()
    for kw in keywords:
        if kw in text:
            return True
    return False


def find_best_key_for_element(driver, elem):
    combined = attr_texts(elem)
    label_text = text_of_label_for(driver, elem)
    combined = (combined + " " + label_text).lower()
    for key, kws in FIELD_KEYWORDS.items():
        if matches_keywords(combined, kws):
            return key
    typ = (elem.get_attribute("type") or "").lower()
    if typ == "email":
        return "email"
    if typ in ("tel", "tel-national", "tel-local"):
        return "phone"
    if elem.tag_name.lower() == "textarea":
        return "message"
    return None


def _setup_chrome_options():
    options = Options()
    # options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    # unique_id = str(uuid.uuid4())[:9]
    # timestamp = str(int(time.time() * 1000))
    # profile_dir = tempfile.mkdtemp(prefix=f'chrome_selenium_{unique_id}_{timestamp}_')
    # options.add_argument(f'--user-data-dir={profile_dir}')

    # try common chrome binary locations
    # for path in ['/usr/bin/google-chrome', '/usr/bin/google-chrome-stable', '/opt/google/chrome/google-chrome', '/usr/bin/chromium-browser']:
    #     if os.path.exists(path):
    #         options.binary_location = path
    #         break

    options.add_argument('--disable-gpu')
    options.add_argument('--disable-extensions')
    return options


def generate_random_date_from_1995():
    from datetime import date, timedelta
    _rand = random.Random()
    start = date(1995, 1, 1)
    end = date.today()
    delta = (end - start).days
    if delta <= 0:
        return end.strftime("%Y-%m-%d")
    r = _rand.randint(0, delta)
    d = start + timedelta(days=r)
    return d.strftime("%Y-%m-%d")


def build_form_payload_from_row(row: Dict[str, Any], generated_message: str) -> Dict[str, Any]:
    name = row.get('full_name') or ' '.join(filter(None, [row.get('first_name'), row.get('last_name')]))
    payload = {}
    if name:
        payload['name'] = name
    if row.get('company_name'):
        payload['company'] = row.get('company_name')
    if row.get('email_address'):
        payload['email'] = row.get('email_address')
    if row.get('phone_number'):
        payload['phone'] = row.get('phone_number')
    payload['message'] = row.get('personalized_message') or generated_message or f"Hello, I'm interested in your services on {row.get('website_url','')}"
    payload['subject'] = row.get('campaign_name') or 'Business Inquiry'
    return payload


# --- DB helper (standalone, optional) ---


def _get_db_conn():
    if not PSYCOPG2_AVAILABLE:
        logger.warning(f"PSYCOPG2_not AVAILABLE: ")
        return None
    database_url = os.getenv('DATABASE_URL')
    try:
        if database_url:
            return psycopg2.connect(database_url)
        conn_args = dict(
            host=os.getenv('DB_HOST', 'localhost'),
            port=int(os.getenv('DB_PORT', 5432)),
            dbname=os.getenv('DB_NAME', ''),
            user=os.getenv('DB_USER', ''),
            password=os.getenv('DB_PASSWORD', '')
        )
        # remove empty values
        conn_args = {k: v for k, v in conn_args.items() if v}
        return psycopg2.connect(**conn_args)
    except Exception as e:
        logger.warning(f"Could not connect to DB: {e}")
        return None


def update_contact_status(contact_id: str, status: str,final_status:str, submission_time: datetime):
    """Update contact_urls.form_status and submission_time if DB available."""
    conn = _get_db_conn()
    if not conn:
        logger.debug("No DB connection available; skipping update")
        return False
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE contact_urls
            SET form_status = %s,status = %s, submission_time = %s, updated_at = NOW()
            WHERE id = %s
            """,
            (status,final_status, submission_time, contact_id)
        )
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"DB update failed for {contact_id}: {e}")
        try:
            conn.close()
        except Exception:
            pass
        return False


# --- Main exported function ---


def submit_contact_form_old(form_data: Dict[str, Any], generated_message: str,job, user_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Submit a contact form (standalone).

    form_data: expects keys like `form_url`, optional `field_mapping`, and optional `id`/`contact_id` to update DB.
    """
    form_data1 = {
        'field_mapping': {
            'name': '//input[contains(translate(@name,"ABCDEFGHIJKLMNOPQRSTUVWXYZ","abcdefghijklmnopqrstuvwxyz"),"name")] | //input[contains(translate(@id,"ABCDEFGHIJKLMNOPQRSTUVWXYZ","abcdefghijklmnopqrstuvwxyz"),"name")] | //input[contains(translate(@placeholder,"ABCDEFGHIJKLMNOPQRSTUVWXYZ","abcdefghijklmnopqrstuvwxyz"),"name")]',
            'email': '//input[contains(translate(@name,"ABCDEFGHIJKLMNOPQRSTUVWXYZ","abcdefghijklmnopqrstuvwxyz"),"email")] | //input[contains(translate(@id,"ABCDEFGHIJKLMNOPQRSTUVWXYZ","abcdefghijklmnopqrstuvwxyz"),"email")] | //input[@type="email"]',
            'phone': '//input[contains(translate(@name,"ABCDEFGHIJKLMNOPQRSTUVWXYZ","abcdefghijklmnopqrstuvwxyz"),"phone")] | //input[contains(translate(@id,"ABCDEFGHIJKLMNOPQRSTUVWXYZ","abcdefghijklmnopqrstuvwxyz"),"phone")] | //input[@type="tel"]',
            'message': '//textarea[contains(translate(@name,"ABCDEFGHIJKLMNOPQRSTUVWXYZ","abcdefghijklmnopqrstuvwxyz"),"message")] | //textarea[contains(translate(@id,"ABCDEFGHIJKLMNOPQRSTUVWXYZ","abcdefghijklmnopqrstuvwxyz"),"message")] | //textarea[contains(translate(@placeholder,"ABCDEFGHIJKLMNOPQRSTUVWXYZ","abcdefghijklmnopqrstuvwxyz"),"message")]',
            'subject': '//input[contains(translate(@name,"ABCDEFGHIJKLMNOPQRSTUVWXYZ","abcdefghijklmnopqrstuvwxyz"),"subject")] | //input[contains(translate(@id,"ABCDEFGHIJKLMNOPQRSTUVWXYZ","abcdefghijklmnopqrstuvwxyz"),"subject")] | //select[contains(translate(@name,"ABCDEFGHIJKLMNOPQRSTUVWXYZ","abcdefghijklmnopqrstuvwxyz"),"subject")]',
            'company': '//input[contains(translate(@name,"ABCDEFGHIJKLMNOPQRSTUVWXYZ","abcdefghijklmnopqrstuvwxyz"),"company")] | //input[contains(translate(@id,"ABCDEFGHIJKLMNOPQRSTUVWXYZ","abcdefghijklmnopqrstuvwxyz"),"company")] | //input[contains(translate(@placeholder,"ABCDEFGHIJKLMNOPQRSTUVWXYZ","abcdefghijklmnopqrstuvwxyz"),"company")]'
        }
    }

    default_user_config = {
        'sender_name':  form_data.get('full_name'),
        'sender_email': form_data.get('email_address'),
        'sender_phone': form_data.get('phone_number'),
        'message_subject': form_data.get('personalized_message'),
        'company_name': form_data.get('company_name')
    }
    cfg = {**default_user_config, **(user_config or {})}

    contact_id = form_data.get('id') or form_data.get('contact_id')
    contact_row_like = {
        'full_name': form_data.get('full_name'),
        'first_name': form_data.get('first_name'),
        'last_name': form_data.get('last_name'),
        'company_name': form_data.get('company_name'),
        'email_address': form_data.get('email_address'),
        'phone_number': form_data.get('phone_number'),
        'website_url': form_data.get('website_url'),
        'personalized_message': form_data.get('personalized_message'),
        'campaign_name': form_data.get('campaign_name')
    }

    # Try Selenium-based submission first if available
    if SELENIUM_AVAILABLE:
        chrome_options = _setup_chrome_options()
        driver = None
        out = {"filled": {}, "submitted": False, "notes": []}
        try:
            # driver = webdriver.Chrome(options=chrome_options)
            from selenium.webdriver.chrome.service import Service
            from webdriver_manager.chrome import ChromeDriverManager
            logger.info(f"Going TO opend Driver : {form_data['form_url']}")
            driver = webdriver.Chrome(
                service=Service(ChromeDriverManager().install()),
                options=chrome_options
            )
            driver.get(form_data['form_url'])
            time.sleep(3)

            try:
                Accept = driver.find_element(By.XPATH, '//button[@aria-label="Accept All"]')
                Accept.click()
            except:
                pass
            # Try to fill mapped fields if provided
            field_mapping = form_data1.get('field_mapping', {})

            # Basic prepared data
            data = {
                'name': cfg['sender_name'],
                'email': cfg['sender_email'],
                'subject': cfg['message_subject'],
                'message': form_data.get('personalized_message'),
                'description': form_data.get('personalized_message'),
                'describe': form_data.get('personalized_message'),
                'about': form_data.get('personalized_message'),
                'phone': cfg['sender_phone'],
                'company': cfg['company_name']
            }

            # # Find inputs
            # try:
            #     elems = driver.find_elements(By.XPATH, "//input|//textarea|//select")
            # except Exception:
            #     elems = []
            #
            # for elem in elems:
            #     try:
            #         if not elem.is_displayed():
            #             continue
            #     except Exception:
            #         continue
            #     key = find_best_key_for_element(driver, elem)
            #     typ = (elem.get_attribute('type') or '').lower()
            #     tag = elem.tag_name.lower()
            #
            #     # handle basics similar to original
            #     if tag == 'input' and typ == 'file':
            #         continue
            #     if typ == 'checkbox':
            #         lbl = text_of_label_for(driver, elem).lower()
            #         if 'subscribe' in lbl and data.get('subscribe'):
            #             try:
            #                 if not elem.is_selected():
            #                     elem.click()
            #             except Exception:
            #                 pass
            #         continue
            #     if typ == 'radio':
            #         try:
            #             elem.click()
            #         except Exception:
            #             pass
            #         continue
            #     if tag == 'select':
            #         try:
            #             sel = Select(elem)
            #             options = sel.options
            #             if key and key in data and data[key]:
            #                 for o in options:
            #                     if (o.text or '').lower().find(str(data[key]).lower()) != -1:
            #                         sel.select_by_visible_text(o.text)
            #                         break
            #             else:
            #                 for o in options:
            #                     if o.get_attribute('value'):
            #                         sel.select_by_visible_text(o.text)
            #                         break
            #         except Exception:
            #             pass
            #         continue
            #
            #     # text fields
            #     guess = key
            #     if not guess:
            #         attrs = attr_texts(elem)
            #         for k in data.keys():
            #             if k in FIELD_KEYWORDS and matches_keywords(attrs, FIELD_KEYWORDS[k]):
            #                 guess = k
            #                 break
            #     if not guess:
            #         placeholder = (elem.get_attribute('placeholder') or '').lower()
            #         if 'message' in placeholder:
            #             guess = 'message'
            #     if guess and guess in data and data[guess] is not None:
            #         try:
            #             try:
            #                 elem.clear()
            #             except Exception:
            #                 pass
            #             elem.click()
            #             elem.send_keys(str(data[guess]))
            #         except Exception:
            #             pass
            #
            # # try submit
            # try:
            #     submit_btn = driver.find_element(By.CSS_SELECTOR, "form input[type=submit], form button[type=submit]")
            #     driver.execute_script("arguments[0].scrollIntoView(true);", submit_btn)
            #     submit_btn.click()
            # except Exception:
            #     try:
            #         driver.execute_script("document.querySelector('form').submit();")
            #     except Exception:
            #         pass
            #
            # time.sleep(5)
            # page_text = (driver.page_source or '').lower()
            # success = any(k in page_text for k in ("thank you", "success", "submitted", "received", "sent"))
            #
            # submission_time = datetime.utcnow()
            # # Update DB if contact id present
            # if contact_id:
            #     update_contact_status(contact_id, 'COMPLETED' if success else 'FAILED', submission_time)
            #
            # return {
            #     'success': success,
            #     'submission_time': submission_time,
            #     'response_page': (driver.page_source or '')[:2000],
            #     'form_url': form_data.get('form_url')
            # }

            print(data, "filling this - - - -")
            logger.info(f"filling this - - - - : {data}")
            name_=False
            if field_mapping.get('name'):

                try:
                    name_field = driver.find_element(By.XPATH, field_mapping['name'])
                    time.sleep(0.5)
                    name_field.clear()
                    time.sleep(0.5)
                    name_field.send_keys(cfg['sender_name'])
                    logger.info(f"Filled subject field: {cfg['sender_name']}")
                    name_ = True
                except Exception as e:
                    logger.warning(f"Could not fill subject field: {e}")



            # try:
            #     elements = driver.find_elements(By.XPATH, "//input|//textarea|//select")
            # except Exception:
            #     elements = []
            elements = []
            last_height = driver.execute_script("return document.body.scrollHeight")

            while True:
                try:
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    elements = driver.find_elements(By.XPATH, "//input|//textarea|//select")
                    if elements:
                        break  # ✅ elements found → exit loop
                except Exception:
                    pass

                # scroll down
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)

                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break  # ❌ reached bottom → stop loop

                last_height = new_height

            try:
                main_field = driver.find_element(By.XPATH, field_mapping['name'])
            except:
                main_field=[]
            try:
                main_field2 = driver.find_element(By.XPATH, "//form")
            except:
                main_field2=[]



            submit_buttons = []
            count_scroll = 0

            first_radio_selected = False

            if not elements and not main_field2 and not main_field:
                result = {
                    'success': False,
                    'submission_time': datetime.now(),
                    'error': 'Form Not found',
                    'response_page': driver.page_source[:1000],  # First 1000 chars
                    'form_url': form_data['form_url']
                }

                logger.info(f"Form Not found - - - - : {result}")
                submission_time = datetime.utcnow()
                if contact_id:
                    # update_contact_status(contact_id, 'FORM NOT FOUND','FORM NOT FOUND', submission_time)
                    update_aws_job_metadata(
                        job['id'],
                        status="FORM NOT FOUND",
                        completed=True,job=job
                    )

                return result

            for elem in elements:


                try:
                    if not elem.is_displayed():
                        continue
                except Exception as e:
                    continue

                tag = elem.tag_name.lower()
                typ = (elem.get_attribute("type") or "").lower()
                if not name_:
                    pass
                    # Scroll up a little
                    # driver.execute_script("window.scrollBy(0, -200);")
                    # time.sleep(0.5)

                # skip hidden / non-interactive
                if typ in ("hidden", "submit", "button", "image"):
                    if typ == "submit" and elem.is_displayed():
                        submit_buttons.append(elem)
                    continue

                key = find_best_key_for_element(driver, elem)

                # --- file upload ---
                if tag == "input" and typ == "file":
                    key = key or "file"
                    if key in data and data[key]:
                        try:
                            elem.send_keys(data[key])
                            out["filled"][key] = data[key]
                        except Exception as e:
                            out["notes"].append(f"file upload failed: {e}")
                    continue

                # --- checkboxes ---
                if typ == "checkbox":
                    label = text_of_label_for(driver, elem).lower()
                    if elem:
                        try:
                            if not elem.is_selected():
                                elem.click()
                            out["filled"]["subscribe"] = True
                        except Exception as e:
                            out["notes"].append(f"checkbox click failed: {e}")
                    continue

                # --- radio buttons: select first radio ---
                if typ == "radio" and not first_radio_selected:
                    try:
                        elem.click()
                        first_radio_selected = True
                        out["filled"]["radio_selected"] = "first"
                    except Exception as e:
                        out["notes"].append(f"first radio click failed: {e}")
                    continue

                # --- select dropdown ---
                if tag == "select":
                    try:
                        sel = Select(elem)
                        options = sel.options
                        chosen = None
                        if key in data:
                            for o in options:
                                if data[key].lower() in o.text.lower():
                                    chosen = o
                                    break
                        if not chosen:
                            for o in options:
                                if o.get_attribute("value") and not o.get_attribute("disabled"):
                                    chosen = o
                                    break
                        if chosen:
                            sel.select_by_visible_text(chosen.text)
                            out["filled"].setdefault("selects", {})[
                                elem.get_attribute("name") or elem.get_attribute("id") or chosen.text] = chosen.text
                    except Exception as e:
                        out["notes"].append(f"select error: {e}")
                    continue

                # --- text inputs and textareas ---
                guess = key
                if not guess:
                    attrs = attr_texts(elem)
                    for k in data.keys():
                        if k in FIELD_KEYWORDS and matches_keywords(attrs, FIELD_KEYWORDS[k]):
                            guess = k
                            break

                if not guess:
                    placeholder = (elem.get_attribute("placeholder") or "").lower()
                    if len(placeholder) < 30 and "message" in placeholder:
                        guess = "message"

                # if guess and guess in data and data[guess] is not None:
                #     try:
                #         try:
                #             elem.clear()
                #         except Exception:
                #             pass
                #         elem.click()
                #         elem.send_keys(str(data[guess]))
                #         out["filled"][guess] = data[guess]
                #     except Exception as e:
                #         out["notes"].append(f"couldn't fill {guess}: {e}")

                if guess and guess in data and data[guess] is not None:
                    try:
                        # try scrolling up to 10 times
                        for _ in range(10):
                            count_scroll+=1
                            if count_scroll == 1:
                                driver.execute_script("window.scrollBy(0, -300);")
                            else:
                                driver.execute_script(
                                    "arguments[0].scrollIntoView({block: 'center'});",
                                    elem
                                )
                            try:

                                try:
                                    elem.clear()
                                except Exception:
                                    print("cleaning error - - - -")
                                    pass
                                elem.click()
                                elem.send_keys(str(data[guess]))
                                out["filled"][guess] = data[guess]

                                print(f"cleaning error - - - -{data[guess]} ,{str(data[guess])} ,{count_scroll}")


                                break  # ✅ success, stop scrolling

                            except Exception:
                                driver.execute_script(
                                    "arguments[0].scrollIntoView({block: 'center'});",
                                    elem
                                )
                        else:
                            # runs only if loop never broke
                            raise Exception("element not interactable after scrolling")

                    except Exception as e:
                        out["notes"].append(f"couldn't fill {guess}: {e}")

            # Try to fill common DOB / date fields and other custom widgets before main mapping
            try:
                # Quick helper to safely click/send keys
                def safe_send_keys(el, value):
                    try:
                        try:
                            el.clear()
                        except Exception:
                            pass
                        el.click()
                        time.sleep(0.05)
                        el.send_keys(value)
                        return True
                    except Exception:
                        return False

                # 1) Fill date inputs and inputs that look like DOB fields
                date_xpath = (
                    "//input[@type='date'] |"
                    "//input[contains(translate(@name,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'dob')] |"
                    "//input[contains(translate(@id,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'dob')] |"
                    "//input[contains(translate(@placeholder,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'dob')] |"
                    "//input[contains(translate(@name,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'birth')] |"
                    "//input[contains(translate(@placeholder,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'birth')]"
                )
                try:
                    date_inputs = driver.find_elements(By.XPATH, date_xpath)
                except Exception:
                    date_inputs = []

                for di in date_inputs:
                    try:
                        if not di.is_displayed():
                            continue
                        val = generate_random_date_from_1995()
                        if safe_send_keys(di, val):
                            out.setdefault("filled", {}).setdefault("dates", {})[
                                di.get_attribute("name") or di.get_attribute(
                                    "id") or f"date_{len(out.get('filled', {}).get('dates', {})) + 1}"
                                ] = val
                            logger.info(f"Filled date/DOB field with {val}")
                    except Exception as e:
                        logger.debug(f"Could not fill date field: {e}")

                # 2) Inputs that include a YYYY-MM-DD pattern in @pattern attribute or placeholder
                try:
                    pattern_inputs = driver.find_elements(By.XPATH,
                                                          "//input[contains(@pattern,'[0-9]{4}-[0-9]{2}-[0-9]{2}')] | //input[contains(translate(@placeholder,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'-') and (contains(translate(@placeholder,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'yyyy') or contains(translate(@placeholder,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'YYYY') or contains(translate(@placeholder,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'yyyy-mm-dd'))")
                except Exception:
                    pattern_inputs = []

                for pi in pattern_inputs:
                    try:
                        if not pi.is_displayed():
                            continue
                        val = generate_random_date_from_1995()
                        if safe_send_keys(pi, val):
                            out.setdefault("filled", {}).setdefault("dates", {})[
                                pi.get_attribute("name") or pi.get_attribute(
                                    "id") or f"date_pattern_{len(out.get('filled', {}).get('dates', {})) + 1}"
                                ] = val
                            logger.info(f"Filled pattern date field with {val}")
                    except Exception as e:
                        logger.debug(f"Could not fill pattern date field: {e}")

                # 3) UL-based dropdowns: try to click the UL (if it looks like a dropdown) and then click the first visible <li>
                try:
                    uls = driver.find_elements(By.XPATH, "//ul[./li]")
                except Exception:
                    uls = []

                for ul in uls:
                    try:
                        if not ul.is_displayed():
                            continue
                        cls = (ul.get_attribute('class') or '').lower()
                        role = (ul.get_attribute('role') or '').lower()
                        if not ("dropdown" in cls or "select" in cls or role in ('listbox', 'menu')):
                            # skip generic lists that don't look like dropdowns
                            continue

                        # Try to open and click first visible li
                        try:
                            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", ul)
                            ul.click()
                            time.sleep(0.15)
                        except Exception:
                            pass

                        lis = ul.find_elements(By.TAG_NAME, "li")
                        first_li = None
                        for li in lis:
                            try:
                                if li.is_displayed():
                                    first_li = li
                                    break
                            except Exception:
                                continue

                        if first_li:
                            try:
                                first_li.click()
                                out.setdefault("filled", {}).setdefault("selects", {})[
                                    ul.get_attribute('id') or ul.get_attribute(
                                        'class') or f"ul_select_{len(out.get('filled', {}).get('selects', {})) + 1}"
                                    ] = first_li.text or first_li.get_attribute('data-value') or 'first_option'
                                logger.info("Clicked first option in UL-based dropdown")
                            except Exception as e:
                                logger.debug(f"Could not click UL dropdown option: {e}")
                    except Exception:
                        continue

                # 4) Radio groups: select the first visible radio for each name group
                try:
                    radios = driver.find_elements(By.CSS_SELECTOR, "input[type='radio']")
                except Exception:
                    radios = []

                radios_by_name = {}
                for r in radios:
                    try:
                        name = r.get_attribute('name') or '__noname__'
                        radios_by_name.setdefault(name, []).append(r)
                    except Exception:
                        continue

                for name, group in radios_by_name.items():
                    for r in group:
                        try:
                            if r.is_displayed() and r.is_enabled():
                                r.click()
                                out.setdefault("filled", {}).setdefault("radios", {})[name] = 'first_selected'
                                logger.info(f"Selected first radio for group {name}")
                                break
                        except Exception:
                            continue

                # 5) Accept/Agree handling: check checkboxes or click accept buttons
                try:
                    checks = driver.find_elements(By.CSS_SELECTOR, "input[type='checkbox']")
                except Exception:
                    checks = []

                for cb in checks:
                    try:
                        if not cb.is_displayed():
                            continue
                        lbl = ''
                        try:
                            lbl = text_of_label_for(driver, cb).lower()
                        except Exception:
                            pass
                        if any(k in (lbl or '') for k in ("accept", "agree", "terms", "consent", "i agree")):
                            try:
                                if not cb.is_selected():
                                    cb.click()
                                    out.setdefault("filled", {}).setdefault("checkboxes", {})[
                                        cb.get_attribute('name') or cb.get_attribute(
                                            'id') or f"accept_{len(out.get('filled', {}).get('checkboxes', {})) + 1}"
                                        ] = True
                                    logger.info("Clicked accept/agree checkbox")
                            except Exception as e:
                                logger.debug(f"Could not click accept checkbox: {e}")
                    except Exception as e:
                        logger.debug(f"erroor: {e}")
                # Click buttons/links that look like accept/agree
                try:
                    btns = driver.find_elements(By.XPATH, "//button|//a")
                    for b in btns:
                        try:
                            if not b.is_displayed():
                                continue
                            txt = (b.text or '').strip().lower()
                            if any(k in txt for k in (
                            "accept", "agree", "i agree", "accept all", "accept cookies", "agree and continue",
                            "i accept")):
                                try:
                                    b.click()
                                    logger.info("Clicked accept/agree button")
                                except Exception:
                                    continue
                        except Exception:
                            continue
                except Exception:
                    pass

            except Exception as e:
                logger.debug(f"Post-fill helper failed: {e}")
            if field_mapping.get('subject'):
                try:
                    subject_field = driver.find_element(By.XPATH, field_mapping['subject'])
                    time.sleep(0.5)
                    subject_field.clear()
                    time.sleep(0.5)
                    subject_field.send_keys(cfg['message_subject'])
                    logger.info(f"Filled subject field: {cfg['message_subject']}")
                except Exception as e:
                    logger.warning(f"Could not fill subject field: {e}")



            if field_mapping.get('message'):
                try:
                    message_field = driver.find_element(By.XPATH, field_mapping['message'])
                    time.sleep(0.5)
                    message_field.clear()
                    time.sleep(0.5)
                    message_field.send_keys(form_data.get('personalized_message'))
                    logger.info(f"Filled message field with generated message")
                except Exception as e:
                    logger.warning(f"Could not fill message field: {e}")
            try:
                def inject_recaptcha_response(driver, token):
                    driver.execute_script(f'document.getElementById("g-recaptcha-response").innerHTML="{token}";')
                    driver.execute_script(
                        'document.getElementById("g-recaptcha-response").dispatchEvent(new Event("change"));')

                def solve_recaptcha(site_key, url):
                    logger.info(f"Requesting to solve captcha $$$$$$$$$$$")
                    s = requests.Session()
                    captcha_id = s.post(
                        "http://2captcha.com/in.php",
                        data={
                            "key": API_KEY_2CAPTCHA,
                            "method": "userrecaptcha",
                            "googlekey": site_key,
                            "pageurl": url,
                            "json": 1
                        }
                    ).json()["request"]

                    recaptcha_answer = None
                    for i in range(15):
                        time.sleep(5)
                        logger.info(f"Check to solve captcha is solve or not $$$$$$$$$$$")
                        resp = s.get(
                            f"http://2captcha.com/res.php?key={API_KEY_2CAPTCHA}&action=get&id={captcha_id}&json=1").json()
                        print(resp)
                        if resp["status"] == 1:
                            logger.info(f"$$$$$$$ captcha_info_Check the status solve if 1: {resp['status']}")
                            recaptcha_answer = resp["request"]
                            break
                    return recaptcha_answer

                frames = driver.find_elements(By.TAG_NAME, "iframe")
                logger.info(f"captcha_info: {frames}")
                logger.info(f"going to solve captcha: {frames}")
                recaptcha_frame = None
                site_key = None
                for f in frames:
                    src = f.get_attribute("src")
                    if src and "google.com/recaptcha" in src:
                        recaptcha_frame = f
                        import urllib.parse as urlparse
                        parsed = urlparse.urlparse(src)
                        params = urlparse.parse_qs(parsed.query)
                        site_key = params.get("k", [None])[0]
                        break
                if recaptcha_frame and site_key:
                    token = solve_recaptcha(site_key, driver.current_url)
                    if token:
                        inject_recaptcha_response(driver, token)
                        logger.info("reCAPTCHA solved via 2Captcha - - - -")
            except Exception as e:
                logger.info(f"reCAPTCHA handling failed: {e}")

            try:
                name2_field = driver.find_element(By.XPATH,
                                                  """//label[contains(text(),"Last Name")]//following-sibling::input""")

                name2_field.clear()
                name2_field.send_keys(form_data.get('last_name'))
                logger.info(f"Filled Last name field: {form_data.get('last_name')}")
            except Exception as e:
                logger.warning(f"Could not Last name field: {e}--- {field_mapping['name']}")

            try:
                name3_field = driver.find_element(By.XPATH,
                                                  """//label[contains(text(),"Company")]//following-sibling::input""")
                time.sleep(0.5)
                name3_field.clear()
                time.sleep(0.5)
                name3_field.send_keys(form_data.get('company_name'))
                logger.info(f"Filled Last name field: {form_data.get('company_name')}")
            except Exception as e:
                logger.warning(f"Could not Last name field: {e}--- {field_mapping['name']}")

            # try:
            #     # Wait until at least one radio button or checkbox is clickable
            #     first_input = WebDriverWait(driver, 10).until(
            #         EC.element_to_be_clickable((By.CSS_SELECTOR, "input[type='radio']"))
            #     )
            #     time.sleep(0.5)
            #     # Click the first one
            #     first_input.click()
            #     time.sleep(0.5)
            #     print("Clicked the first radio button or checkbox found!")
            # except:
            #     print("No radio button or checkbox found.")

            logger.info(f"Waiting for 5 seconds - - -{form_data['form_url']}")
            time.sleep(5)
            # if count_scroll>=3:
            #     driver.execute_script("window.scrollBy(0, 300);")
            #     time.sleep(0.5)
            # else:
            driver.execute_script("window.scrollBy(0, 300);")
            time.sleep(0.5)

            try:
                # Only consider submit buttons that are contained within a <form> element
                submit_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable(
                        (By.CSS_SELECTOR, "form input[type='submit'], form button[type='submit']"))
                )

                if submit_button:
                    # driver.execute_script("window.scrollBy(0, 300);")
                    try:
                        driver.execute_script("arguments[0].scrollIntoView(true);", submit_button)
                        submit_button.click()
                        logger.info(f"Form submitted successfully{form_data['form_url']}")
                    except:
                        pass
                    time.sleep(2)

                    driver.execute_script("window.scrollTo(0, 0);")
                    time.sleep(2)
                    nsubmit_button = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable(
                            (By.CSS_SELECTOR, "form input[type='submit'], form button[type='submit']"))
                    )
                    driver.execute_script("arguments[0].scrollIntoView(true);", nsubmit_button)
                    submit_button.click()
                    logger.info(f"Form submitted successfully{form_data['form_url']}")
                    # submit_button.click()
                    logger.info(f"submit_buttons 1 - -- -Form submitted successfully {form_data['form_url']}")
                time.sleep(2)
                if not submit_button:
                    driver.execute_script("window.scrollTo(0, 300);")
                    time.sleep(0.5)
                    submit_buttons = driver.find_elements(By.XPATH,
                                                          "//button[@type='submit' or contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), 'send') or contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), 'submit')]")
                    time.sleep(2)
                    logger.info(f"submit_buttons 2  - -- -Form submitted successfully {form_data['form_url']}")

            except Exception as e:
                logger.info("Retry to submit",e)
                driver.execute_script("window.scrollTo(0, 0);")

                try:

                    try:
                        try:
                            submit_button = WebDriverWait(driver, 10).until(
                                EC.element_to_be_clickable(
                                    (By.CSS_SELECTOR, "form input[type='submit'], form button[type='submit']"))
                            )
                            if submit_button:
                                driver.execute_script("window.scrollBy(0, -300);")
                                driver.execute_script("arguments[0].scrollIntoView(true);", submit_button)
                                submit_button.click()
                                logger.info(f"Form submitted successfully{form_data['form_url']}")
                                time.sleep(2)
                                # submit_button.click()
                                logger.info(f"submit_buttons 1 - -- -Form submitted successfully {form_data['form_url']}")
                        except:
                            time.sleep(2)
                            driver.execute_script("window.scrollTo(0, 300);")

                            submit_buttons = driver.find_elements(By.XPATH,
                                                                  "//button[@type='submit' or contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), 'send') or contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), 'submit')]")
                            time.sleep(2)
                            logger.info(f"submit_buttons 2  - -- -Form submitted successfully {form_data['form_url']}")

                    except:
                        driver.execute_script("window.scrollBy(0, 200);")
                        submit_button = driver.find_element(By.CSS_SELECTOR,
                                                            "button:contains('Send'), button:contains('Submit')")
                        submit_button.click()
                        logger.info(f"DD &&&&& Submitted Successful...{form_data['form_url']}")
                except:
                    try:
                        logger.info("  advancesd - -- - -")
                        driver.execute_script("document.querySelector('form').submit();")
                        logger.info(" submitted 3 with advancesd - -- - -")
                        logger.info(f"Form 3 submitted successfully{form_data['form_url']}")

                    except Exception as e:

                        try:
                            if not submit_buttons:

                                for btn in driver.find_elements(By.TAG_NAME, "button"):

                                    txt = (btn.text or "").lower()
                                    if any(k in txt for k in SUBMIT_TEXT_KEYWORDS) and btn.is_displayed():
                                        submit_buttons.append(btn)


                        except:
                            try:

                                submit_button = driver.find_element(By.CSS_SELECTOR,
                                                                    "input[type='submit'], button[type='submit']")
                                driver.execute_script("arguments[0].scrollIntoView(true);", submit_button)
                                time.sleep(2)
                                submit_button.click()
                                logger.info(f"Form submitted successfully{form_data['form_url']}")
                            except:
                                # Submit form
                                try:
                                    logger.info("  advancesd - -- - -")
                                    driver.execute_script("document.querySelector('form').submit();")
                                    logger.info(" submitted with advancesd - -- - -")
                                except Exception as e:
                                    logger.info("Retry to submit")
                                    try:

                                        # Only consider submit buttons that are contained within a <form> element
                                        submit_button = WebDriverWait(driver, 10).until(
                                            EC.element_to_be_clickable((By.CSS_SELECTOR,
                                                                        "form input[type='submit'], form button[type='submit']"))
                                        )
                                        submit_button.click()
                                        logger.info(f"Form submitted successfully{form_data['form_url']}")
                                    except Exception as e:
                                        logger.warning(f"Could not find submit button retry once more - - -: {e}")

                                        # Try alternative submit methods

                                        try:
                                            submit_button = driver.find_element(By.CSS_SELECTOR,
                                                                                "button:contains('Send'), button:contains('Submit')")
                                            submit_button.click()
                                            logger.info(f"DD &&&&& Submitted Successful...{form_data['form_url']}")
                                        except Exception as eeee:
                                            try:
                                                print("submittintt through adavnce -")
                                                driver.execute_script("document.querySelector('form').submit();")
                                                print("submittintt through adavnce Done - - - -")
                                            except Exception as e:
                                                e=f'Failed To submit Please verify...{form_data['form_url']} {str(e)}'
                                                mark_failed(job['id'], str(e))
                                                driver.quit()
                                                logger.info(
                                                    f"DD &&&&& Failed To submit Please verify...{form_data['form_url']} {e}")
                                                return {
                                                    'success': False,
                                                    'error': f'Selenium failed: {eeee}.  submission not done no result.',
                                                    'submission_time': datetime.now(),
                                                    'form_url': form_data.get('form_url', '')
                                                }

            # Wait for submission
            time.sleep(5)

            # Check for success indicators
            success_indicators = [
                "thank you",
                "success",
                "submitted",
                "received",
                "sent"
            ]

            try:
                thank_u_message_field = driver.find_element(By.XPATH,
                                                            '''"//*[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'thank you') or contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'success') or contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'submitted') or contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'received')]"''')
                logger.info(f"Thank you appered - -  - - - - : {thank_u_message_field}")
            except:
                logger.info(f"Not appered thank you- -  - - - - ")

            page_text = driver.page_source.lower()
            submission_successful = any(indicator in page_text for indicator in success_indicators)

            result = {
                'success': submission_successful,
                'submission_time': datetime.now(),
                'response_page': driver.page_source[:1000],  # First 1000 chars
                'form_url': form_data['form_url']
            }

            logger.info(f"All Form Submitted - - - - : {result}")
            submission_time = datetime.utcnow()
            if contact_id:
                # update_contact_status(contact_id, 'COMPLETED','DONE', submission_time)
                update_aws_job_metadata(
                    job['id'],
                    status="COMPLETED",
                    completed=True,job=job
                )

            return result

        except Exception as e:
            submission_time = datetime.utcnow()
            logger.error(f"Selenium submission error: {e} {submission_time}")
            e = f'Selenium submission error...{submission_time}{form_data['form_url']} {str(e)}'
            mark_failed(job['id'], str(e))

            # update_contact_status(contact_id, 'FAILED','FAILED', submission_time)
            update_aws_job_metadata(
                job['id'],
                status="FAILED",
                completed=True,job=job
            )
            return {
                'success': False,
                'error': f'Selenium failed: {e}.  submission not done no result.',
                'submission_time': datetime.now(),
                'form_url': form_data.get('form_url', '')
            }
            # fallthrough to requests fallback
        finally:
            try:
                if driver:
                    driver.quit()
            except Exception:
                pass


    # --- Non-selenium fallback: simple HTTP POST ---
    # try:
    #     dest = form_data.get('contact_us_url') or form_data.get('form_url')
    #     payload = build_form_payload_from_row(contact_row_like, generated_message)
    #     headers = {'User-Agent': 'Mozilla/5.0 (compatible; FormSubmitter/1.0)'}
    #     resp = requests.post(dest, data=payload, headers=headers, timeout=30, allow_redirects=True)
    #     text = (resp.text or '').lower()
    #     success = resp.status_code in (200, 201, 202, 204) or any(k in text for k in ("thank you", "success", "submitted"))
    #
    # except Exception as e:
    #     logger.error(f"HTTP fallback submission failed: {e}")
    #     submission_time = datetime.utcnow()
    #     if contact_id:
    #         update_contact_status(contact_id, 'FAILED', submission_time)
    #     return {
    #         'success': False,
    #         'error': str(e),
    #         'submission_time': submission_time,
    #         'form_url': form_data.get('form_url')
    #     }


def _fetch_pending_rows(limit: int = 50):
    """Return list of dict rows from contact_urls where form_status = 'PENDING'.

    Returns empty list if DB driver not available or on error.
    """
    if not PSYCOPG2_AVAILABLE:
        logger.warning("psycopg2 not available; cannot fetch pending rows")
        return []
    conn = _get_db_conn()
    if not conn:
        logger.warning("No DB connection; cannot fetch pending rows")
        return []
    try:
        try:
            from psycopg2.extras import RealDictCursor
            cur = conn.cursor(cursor_factory=RealDictCursor)
        except Exception:
            cur = conn.cursor()
        cur.execute("SELECT * FROM contact_urls WHERE form_status = 'PENDING' ORDER BY created_at ASC LIMIT %s", (limit,))
        rows = cur.fetchall()
        if hasattr(rows[0] if rows else None, 'keys'):
            # RealDictCursor or dict-like
            result = [dict(r) for r in rows]
        else:
            cols = [c[0] for c in cur.description]
            result = [dict(zip(cols, r)) for r in rows]
        cur.close()
        conn.close()
        return result
    except Exception as e:
        logger.error(f"Failed fetching pending rows: {e}")
        try:
            conn.close()
        except Exception:
            pass
        return []


def process_pending_forms(limit: int = 50, pause_seconds: float = 1.5):
    """Fetch pending contact_urls and submit each form.

    Returns a list of results for each processed row.
    """
    rows = _fetch_pending_rows(limit)
    results = []
    if not rows:
        logger.info("No pending rows to process")
        return results

    for r in rows:
        # Normalize row to expected form_data keys
        form_url = r.get('contact_us_url') or r.get('form_url') or r.get('url') or r.get('website_url')
        form_data = {
            'id': r.get('id'),
            'contact_id': r.get('id'),
            'form_url': form_url,
            'full_name': r.get('full_name'),
            'first_name': r.get('first_name'),
            'last_name': r.get('last_name'),
            'company_name': r.get('company_name'),
            'email_address': r.get('email_address'),
            'phone_number': r.get('phone_number'),
            'website_url': r.get('website_url') or r.get('website'),
            'personalized_message': r.get('personalized_message'),
            'campaign_name': r.get('campaign_name')
        }
        # field_mapping may be stored as JSON string
        fm = r.get('field_mapping')
        if fm:
            try:
                form_data['field_mapping'] = fm if isinstance(fm, dict) else json.loads(fm)
            except Exception:
                form_data['field_mapping'] = {}

        generated_message = r.get('personalized_message') or f"Hello, I'm interested in your services on {form_url or r.get('website_url','')}"

        logger.info(f"Processing pending contact id={r.get('id')} url={form_url}")
        try:
            res = submit_contact_form_old(form_data, generated_message)
        except Exception as e:
            logger.error(f"submit_contact_form_old failed for id={r.get('id')}: {e}")
            res = {'success': False, 'error': str(e)}
        results.append({'id': r.get('id'), 'url': form_url, 'result': res})
        time.sleep(pause_seconds)

    return results
def fetch_and_lock_one_job():
    if not PSYCOPG2_AVAILABLE:
        return None

    conn = _get_db_conn()
    if not conn:
        return None

    try:
        from psycopg2.extras import RealDictCursor
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute("""
            UPDATE contact_urls
            SET form_status = 'PROCESSING',
                worker_id = %s,
                locked_at = NOW()
            WHERE id = (
                SELECT id
                FROM contact_urls
                WHERE form_status = 'PENDING'
                ORDER BY created_at ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING *;
        """, (WORKER_ID,))

        row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        return dict(row) if row else None

    except Exception as e:
        conn.rollback()
        conn.close()
        logger.error(f"Fetch+lock failed: {e}")
        return None

# def recover_stuck_jobs():
#     if not PSYCOPG2_AVAILABLE:
#         return
#
#     conn = _get_db_conn()
#     if not conn:
#         return
#
#     try:
#         cur = conn.cursor()
#         cur.execute("""
#             UPDATE contact_urls
#             SET form_status = 'PENDING',
#                 worker_id = NULL,
#                 locked_at = NULL,
#                 retry_count = retry_count + 1
#             WHERE form_status = 'PROCESSING'
#               AND locked_at < NOW() - INTERVAL '%s minutes'
#               AND retry_count < %s;
#         """, (LOCK_TIMEOUT_MINUTES, MAX_RETRIES))
#         conn.commit()
#         cur.close()
#         conn.close()
#     except Exception as e:
#         conn.close()
#         logger.error(f"Recovery failed: {e}")

def mark_failed(contact_id, error):
    conn = _get_db_conn()
    if not conn:
        return

    cur = conn.cursor()
    cur.execute("""
        UPDATE contact_urls
        SET retry_count = retry_count + 1,
            last_error = %s,
            form_status = CASE
                WHEN retry_count + 1 >= %s THEN 'FAILED'
                ELSE 'Queued'
            END,
            worker_id = NULL,
            locked_at = NULL
        WHERE id = %s;
    """, (error, MAX_RETRIES, contact_id))
    conn.commit()
    conn.close()


def thread_worker():
    while True:
        job = fetch_and_lock_one_job()
        if not job:
            logger.info("No pending jobs left")
            break

        form_url = get_or_scrape_form_url(job) or job.get('form_url') or job.get('website_url')

        form_data = {
            'id': job.get('id'),
            'contact_id': job.get('id'),
            'form_url': form_url,
            'full_name': job.get('full_name'),
            'first_name': job.get('first_name'),
            'last_name': job.get('last_name'),
            'company_name': job.get('company_name'),
            'email_address': job.get('email_address'),
            'phone_number': job.get('phone_number'),
            'website_url': job.get('website_url'),
            'personalized_message': job.get('personalized_message'),
            'campaign_name': job.get('campaign_name')
        }

        try:
            submit_contact_form_old(form_data, job.get('personalized_message'))
        except Exception as e:
            logger.error(f"Job failed {job['id']}: {e}")
            mark_failed(job['id'], str(e))
import boto3
import signal
import sys

QUEUE_URL = os.getenv("QUEUE_URL",'https://sqs.us-east-1.amazonaws.com/957440525184/selenium-worker-jobs')
VISIBILITY_TIMEOUT = 1200  # must be > max selenium execution time
SHUTDOWN = False

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

sqs = boto3.client(
    "sqs",
    region_name=AWS_REGION
)

def shutdown_handler(signum, frame):
    global SHUTDOWN
    SHUTDOWN = True
    logger.info("Shutdown signal received, finishing current job...")

signal.signal(signal.SIGTERM, shutdown_handler)
signal.signal(signal.SIGINT, shutdown_handler)


def mark_done(contact_id):
    conn = _get_db_conn()
    if not conn:
        return
    cur = conn.cursor()
    cur.execute("""
        UPDATE contact_urls
        SET form_status='DONE',
            worker_id=NULL,
            locked_at=NULL,
            updated_at=NOW()
        WHERE id=%s;
    """, (contact_id,))
    conn.commit()
    conn.close()


def recover_stuck_jobs():
    try:
        logger.info(f"Database connectionss: ")
        conn = _get_db_conn()
        if not conn:
            return
        cur = conn.cursor()
        cur.execute("""
            UPDATE contact_urls
            SET form_status='PENDING',
                worker_id=NULL,
                locked_at=NULL,
                retry_count = retry_count + 1
            WHERE form_status='PROCESSING'
              AND locked_at < NOW() - INTERVAL '%s minutes'
              AND retry_count < %s;
        """, (LOCK_TIMEOUT_MINUTES, MAX_RETRIES))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.info(f"Error in recover_stuck_jobs: {e}")


def get_job_by_id(contact_id):
    """Fetch a job row by id without locking."""
    if not PSYCOPG2_AVAILABLE:
        return None
    conn = _get_db_conn()
    if not conn:
        return None
    try:
        from psycopg2.extras import RealDictCursor
        cur = conn.cursor(cursor_factory=RealDictCursor)
    except Exception:
        cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM contact_urls WHERE id = %s", (contact_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            return None
        return dict(row) if hasattr(row, 'keys') else dict(zip([c[0] for c in cur.description], row))
    except Exception as e:
        try:
            conn.close()
        except Exception:
            pass
        logger.error(f"get_job_by_id failed: {e}")
        return None


def try_lock_job(contact_id):
    logger.info(f"Going for connection:")
    conn = _get_db_conn()

    if not conn:
        return None

    from psycopg2.extras import RealDictCursor
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # cur.execute("""
    #     UPDATE contact_urls
    #     SET form_status='PROCESSING',
    #         worker_id=%s,
    #         0=NOW()
    #     WHERE id=%s
    #       AND form_status='PENDING'
    #       AND retry_count < %s
    #     RETURNING *;
    # """, (WORKER_ID, contact_id, MAX_RETRIES))
    cur.execute("""
               UPDATE contact_urls
               SET form_status = 'PROCESSING',
                   worker_id = %s,
                   locked_at = NOW()
               WHERE id = (
                   SELECT id
                   FROM contact_urls
                   WHERE form_status = 'Queued' and id= %s
                   ORDER BY created_at ASC
                   LIMIT 1
                   FOR UPDATE SKIP LOCKED
               )
               RETURNING *;
           """, (WORKER_ID,contact_id,))

    row = cur.fetchone()
    conn.commit()
    conn.close()
    logger.info(f"contact_urls Updated to Pending: {row}")
    return dict(row) if row else None

def get_instance_private_ip():
    try:
        import socket
        # r = requests.get(
        #     "http://169.254.169.254/latest/meta-data/local-ipv4",
        #     timeout=1
        # )
        # logger.info(f"IP Details: {r.text}")
        # return r.text
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip

    except Exception:
        return "unknown"
def update_aws_job_metadata(
    contact_id,
    message_id=None,
    receipt_handle=None,
    status=None,
    started=False,
    completed=False,job=None
):
    conn = _get_db_conn()
    if not conn:
        return

    fields = []
    values = []
    INSTANCE_PRIVATE_IP = get_instance_private_ip()
    if message_id:
        fields.append("sqs_message_id=%s")
        values.append(message_id)

    if receipt_handle:
        fields.append("sqs_receipt_handle=%s")
        values.append(receipt_handle)

    if status:
        fields.append("form_status=%s")
        values.append(status)

    if status:
        fields.append("status=%s")
        values.append(status)

    if started:
        fields.append("worker_started_at=NOW()")

    if completed:
        fields.append("worker_completed_at=NOW()")
        fields.append("submission_time=NOW()")

    if completed:
        try:
            utc_now = datetime.utcnow().replace(tzinfo=pytz.UTC)
            user_timezone = pytz.timezone(job['time_zone'])
            user_completed_time = utc_now.astimezone(user_timezone)
            fields.append("user_completed_time=%s")
            values.append(str(user_completed_time))
        except:
            logger.info(f"Error in time values: {contact_id}")

    fields.extend([
        "sqs_queue_url=%s",
        "aws_region=%s",
        "worker_instance_ip=%s"
    ])

    logger.info(f"Field details to updated DB : {contact_id}")

    values.extend([QUEUE_URL, AWS_REGION, INSTANCE_PRIVATE_IP])

    sql = f"""
        UPDATE contact_urls
        SET {", ".join(fields)}, updated_at=NOW()
        WHERE id=%s
    """
    values.append(contact_id)

    cur = conn.cursor()
    cur.execute(sql, values)
    conn.commit()
    conn.close()


def update_scraping_result(contact_id, found_url=None):
    """Mark scraping as DONE and optionally update contact_us_url."""
    conn = _get_db_conn()
    if not conn:
        return
    try:
        cur = conn.cursor()
        if found_url:
            cur.execute(
                """
                UPDATE contact_urls
                SET contact_us_url = %s,
                    scraping_status = %s,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (found_url, 'DONE', contact_id)
            )
        else:
            cur.execute(
                """
                UPDATE contact_urls
                SET scraping_status = %s,
                    updated_at = NOW()
                WHERE id = %s
                """,
                ('NOT FOUND', contact_id)
            )
        conn.commit()
    except Exception as e:
        logger.warning(f"Failed to update scraping_result for {contact_id}: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


def find_contact_url_in_html(html, base_url):
    """Try to find a contact page URL from HTML. Returns absolute URL or None."""
    candidates = []
    try:
        if LXML_AVAILABLE:
            doc = lh.fromstring(html)
            # look for links with 'contact' in text or href
            nodes = doc.xpath("//a[contains(translate(normalize-space(text()), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'contact')]")
            for n in nodes:
                href = n.get('href')
                if href:
                    candidates.append(href)
            # links with 'contact' in href
            nodes = doc.xpath("//a[contains(translate(@href, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'contact')]")
            for n in nodes:
                href = n.get('href')
                if href:
                    candidates.append(href)
            # forms with action containing contact
            nodes = doc.xpath("//form[contains(translate(@action,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'contact')]")
            for n in nodes:
                action = n.get('action')
                if action:
                    candidates.append(action)
        else:
            # fallback simple parse with string search
            import re
            for m in re.finditer(r"<a[^>]+href=[\'\"]([^\'\"]+)[\'\"][^>]*>(.*?)</a>", html, re.I|re.S):
                href = m.group(1)
                text = re.sub('<[^<]+?>', '', m.group(2) or '').strip().lower()
                if 'contact' in (text or '') or 'contact' in href.lower():
                    candidates.append(href)
    except Exception as e:
        logger.debug(f"HTML parse error when searching for contact url: {e}")

    # normalize and filter candidates
    seen = set()
    for href in candidates:
        try:
            if href.startswith('javascript:') or href.startswith('#'):
                continue
            full = urljoin(base_url, href)
            if full in seen:
                continue
            seen.add(full)
            return full
        except Exception:
            continue
    return None


def validate_url(url):
    """Check that URL returns a successful HTML response."""
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (compatible; LinkChecker/1.0)'}
        # some servers don't like HEAD; try GET
        resp = requests.get(url, headers=headers, allow_redirects=True, timeout=30)
        if resp.status_code and resp.text:
            return True
    except Exception:
        return False
    return False


def get_or_scrape_form_url(job):
    """Return contact_us_url: existing value, or attempt to discover from website via HTTP then Selenium."""
    existing = job.get('contact_us_url')
    if existing:
        return existing

    website = job.get('website_url') or job.get('website')
    if not website:
        return None

    # ensure scheme
    if not urlparse(website).scheme:
        website = 'http://' + website

    found = None
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0'}
        resp = requests.get(website, headers=headers, timeout=30, allow_redirects=True)
        if resp.status_code != 200 and resp.text:
            found = find_contact_url_in_html(resp.text, resp.url)
            if found and validate_url(found):
                update_scraping_result(job.get('id'), found)
                return found
    except Exception as e:
        logger.debug(f"HTTP scrape failed for {website}: {e}")

    # Fallback to Selenium if available
    if SELENIUM_AVAILABLE:
        driver = None
        try:
            chrome_options = _setup_chrome_options()
            from selenium.webdriver.chrome.service import Service
            from webdriver_manager.chrome import ChromeDriverManager
            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
            driver.get(website)
            time.sleep(3)
            html = driver.page_source
            current_url = driver.current_url or website
            found = find_contact_url_in_html(html, current_url)
            if found and validate_url(found):
                update_scraping_result(job.get('id'), found)
                return found
        except Exception as e:
            logger.debug(f"Selenium scrape failed for {website}: {e}")
        finally:
            try:
                if driver:
                    driver.quit()
            except Exception:
                pass

    # mark scraping done even if nothing found
    update_scraping_result(job.get('id'), found)
    return found

def should_run_job(job_row):
    """Return True if job should run based on scheduled_time and time_zone.

    Behavior: get current server UTC time, convert to job's timezone, then compare
    to scheduled_time (interpreted as local time in that timezone). If scheduled_time
    is missing, return True.
    """
    if not job_row:
        return True
    sched = job_row.get('scheduled_time')
    tz_name = job_row.get('time_zone') or job_row.get('timezone')
    if not sched:
        return True

    # Parse scheduled_time if string
    try:
        if isinstance(sched, str):
            if DATEUTIL_AVAILABLE:
                scheduled_dt = dateutil_parser.parse(sched)
            else:
                # try ISO format
                scheduled_dt = datetime.fromisoformat(sched)
        elif isinstance(sched, datetime):
            scheduled_dt = sched
        else:
            return True
    except Exception as e:
        logger.warning(f"Could not parse scheduled_time {sched}: {e}")
        return True

    # Determine timezone object
    tz_obj = None
    if tz_name:
        try:
            if PYTZ_AVAILABLE:
                tz_obj = pytz.timezone(tz_name)
            else:
                from zoneinfo import ZoneInfo
                tz_obj = ZoneInfo(tz_name)
        except Exception as e:
            logger.warning(f"Invalid timezone {tz_name}: {e}")
            tz_obj = None

    # Current server time in UTC
    now_utc = datetime.now(timezone.utc)

    # Convert now to target tz if available
    if tz_obj:
        try:
            now_in_tz = now_utc.astimezone(tz_obj)
        except Exception:
            # pytz requires localize for naive scheduled dt; keep fallback
            now_in_tz = now_utc
    else:
        now_in_tz = now_utc

    # Normalize scheduled_dt: if naive, treat as local time in tz_obj
    if scheduled_dt.tzinfo is None:
        try:
            if PYTZ_AVAILABLE and tz_obj:
                scheduled_local = tz_obj.localize(scheduled_dt)
            elif tz_obj:
                scheduled_local = scheduled_dt.replace(tzinfo=tz_obj)
            else:
                # no tz info: assume UTC
                scheduled_local = scheduled_dt.replace(tzinfo=timezone.utc)
        except Exception:
            scheduled_local = scheduled_dt.replace(tzinfo=timezone.utc)
    else:
        scheduled_local = scheduled_dt

    # Compare now_in_tz (converted) with scheduled_local also converted to same tz
    try:
        if tz_obj and scheduled_local.tzinfo is not None:
            scheduled_in_tz = scheduled_local.astimezone(tz_obj)
        elif tz_obj and scheduled_local.tzinfo is None:
            if PYTZ_AVAILABLE:
                scheduled_in_tz = tz_obj.localize(scheduled_local.replace(tzinfo=None))
            else:
                scheduled_in_tz = scheduled_local.replace(tzinfo=tz_obj)
        else:
            scheduled_in_tz = scheduled_local
    except Exception:
        scheduled_in_tz = scheduled_local

    # finally compare: run if now_in_tz >= scheduled_in_tz
    try:
        run = now_in_tz >= scheduled_in_tz
        if not run:
            logger.info(f"Job {job_row.get('id')} scheduled for {scheduled_in_tz} in tz {tz_name}; current time {now_in_tz}; skipping.")
        return run
    except Exception as e:
        logger.warning(f"Comparison failed: {e}")
        return True
if __name__ == '__main__':





    #todo for production -----------

    logger.info(f"SQS Worker started: {WORKER_ID}")

    # recover_stuck_jobs()
    logger.info(f"Going for sqs message - - - - ")
    Job_Main_global=''
    try:
        running=False
        while not running:
            # job = try_lock_job('15d64445-c8b7-4639-994d-865844fbcce9')
            try:
                logger.info(f"Check for new sqs message - - - - ")

                resp = sqs.receive_message(
                    QueueUrl=QUEUE_URL,
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=20,
                    VisibilityTimeout=VISIBILITY_TIMEOUT
                )
                #for tests
                # resp={}
                # msg={}
                # resp["Messages"][0]="1"
                # msg["ReceiptHandle"]="11"
                # msg["MessageId"]="11"
                logger.info(f"SQS Worker started and details: {resp}")
                try:
                    logger.info(f"SQS Worker started and details: {resp.text}")
                except:
                    pass

                if "Messages" not in resp:
                    logger.info(f"Messages is not there: waiting for new message")
                    time.sleep(5)
                    continue
                    # job = try_lock_job("temp12553")
                    # sqs.send_message(
                    #     QueueUrl=QUEUE_URL,
                    #     MessageBody=json.dumps({"job_id": str(job["id"])})
                    # )
                    # time.sleep(2)
                    # resp = sqs.receive_message(
                    #     QueueUrl=QUEUE_URL,
                    #     MaxNumberOfMessages=1,
                    #     WaitTimeSeconds=20,
                    #     VisibilityTimeout=VISIBILITY_TIMEOUT
                    # )



                msg = resp["Messages"][0]
                receipt = msg["ReceiptHandle"]
                message_id = msg.get("MessageId")

                try:
                    body = json.loads(msg["Body"])
                    contact_id = body["job_id"]
                    logger.info(f"SQS Worker Processing for ID: {contact_id}")
                    # body = ""
                    # contact_id =""
                except Exception:
                    sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt)
                    logger.info(f"SQS Worker Deleted: {WORKER_ID}")
                    continue


                job = try_lock_job(contact_id)
                Job_Main_global=job
                if job:
                    update_aws_job_metadata(
                        job['id'],
                        message_id=message_id,
                        receipt_handle=receipt,
                        status="PROCESSING",
                        started=True
                    )

                # Already processed / taken by another worker
                if not job:
                    sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt)
                    continue

                try:
                    scraped = get_or_scrape_form_url(job)
                    form_url = scraped or job.get('contact_us_url') or job.get('form_url') or job.get('website_url')

                    form_data = {
                        'id': job.get('id'),
                        'contact_id': job.get('id'),
                        'form_url': form_url,
                        'full_name': job.get('full_name'),
                        'first_name': job.get('first_name'),
                        'last_name': job.get('last_name'),
                        'company_name': job.get('company_name'),
                        'email_address': job.get('email_address'),
                        'phone_number': job.get('phone_number'),
                        'website_url': job.get('website_url'),
                        'personalized_message': job.get('personalized_message'),
                        'campaign_name': job.get('campaign_name')
                    }

                    submit_contact_form_old(form_data, job.get('personalized_message'),job)

                    # mark_done(job['id'])
                    # update_aws_job_metadata(
                    #     job['id'],
                    #     status="DONE",
                    #     completed=True
                    # )
                    sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt)

                except Exception as e:
                    logger.error(f"Job failed {job['id']}: {e}")
                    mark_failed(job['id'], str(e))
                    # ❌ Do NOT delete message → SQS retry
            except Exception as e:
                mark_failed(Job_Main_global['id'], str(e))
                logger.info(f"Something went wrong -- - - - {e}")
        logger.info("Worker exiting cleanly")
        # sys.exit(0)
    except Exception as e:
        logger.info(f"some thing wrong {e}")

    # #todo Debug - - ----------------
    # logger.info(f"SQS Worker started: {WORKER_ID}")
    #
    # # recover_stuck_jobs()
    # logger.info(f"Going for sqs message - - - - ")
    #
    # try:
    #     running = False
    #     while not running:
    #         # job = try_lock_job('15d64445-c8b7-4639-994d-865844fbcce9')
    #         try:
    #
    #             job = try_lock_job('22d9a593-f59e-4f5a-89da-76ce26291d8f')
    #
    #             try:
    #                 scraped = get_or_scrape_form_url(job)
    #                 form_url = scraped or job.get('contact_us_url') or job.get('form_url') or job.get('website_url')
    #
    #                 form_data = {
    #                     'id': job.get('id'),
    #                     'contact_id': job.get('id'),
    #                     'form_url': form_url,
    #                     'full_name': job.get('full_name'),
    #                     'first_name': job.get('first_name'),
    #                     'last_name': job.get('last_name'),
    #                     'company_name': job.get('company_name'),
    #                     'email_address': job.get('email_address'),
    #                     'phone_number': job.get('phone_number'),
    #                     'website_url': job.get('website_url'),
    #                     'personalized_message': job.get('personalized_message'),
    #                     'campaign_name': job.get('campaign_name')
    #                 }
    #
    #                 submit_contact_form_old(form_data, job.get('personalized_message'), job)
    #
    #             except Exception as e:
    #                 logger.error(f"Job failed {job['id']}: {e}")
    #                 mark_failed(job['id'], str(e))
    #                 # ❌ Do NOT delete message → SQS retry
    #         except Exception as e:
    #             logger.info(f"Something went wrong -- - - - {e}")
    #     logger.info("Worker exiting cleanly")
    #     # sys.exit(0)
    # except Exception as e:
    #     logger.info(f"some thing wrong {e}")
