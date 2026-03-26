"""
Synthetic Medicare Provider Data Generator
Generates 100,000+ rows of realistic Medicare provider data.
"""

import pandas as pd
import numpy as np
import os
import random
from typing import List, Dict

random.seed(42)
np.random.seed(42)

# -- Constants ------------------------------------------------------------------

SPECIALTIES = [
    "Cardiology", "Orthopedic Surgery", "Oncology", "Psychiatry",
    "Family Medicine", "Internal Medicine", "Neurology", "Dermatology",
    "Gastroenterology", "Pulmonology", "Endocrinology", "Rheumatology",
    "Urology", "Ophthalmology", "Otolaryngology", "Nephrology",
    "Hematology", "Infectious Disease", "Geriatrics", "Allergy/Immunology",
    "Physical Medicine", "Radiology", "Pathology", "Anesthesiology",
    "Emergency Medicine", "Obstetrics/Gynecology", "Pediatrics",
    "Vascular Surgery", "Thoracic Surgery", "Colorectal Surgery"
]

STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
]

# State population weights (proportional to real populations)
STATE_WEIGHTS = {
    "CA": 10, "TX": 9, "FL": 8, "NY": 7, "PA": 5, "IL": 5,
    "OH": 4, "GA": 4, "NC": 4, "MI": 4, "NJ": 4, "VA": 3,
    "WA": 3, "AZ": 3, "TN": 3, "MA": 3, "IN": 3, "MO": 3,
    "MD": 3, "WI": 3, "CO": 2, "MN": 2, "SC": 2, "AL": 2,
    "LA": 2, "KY": 2, "OR": 2, "OK": 2, "CT": 2, "UT": 2,
    "IA": 1, "NV": 1, "AR": 1, "MS": 1, "KS": 1, "NM": 1,
    "NE": 1, "WV": 1, "ID": 1, "HI": 1, "NH": 1, "ME": 1,
    "RI": 1, "MT": 1, "DE": 1, "SD": 1, "ND": 1, "AK": 1,
    "VT": 1, "WY": 1
}

# Base cost parameters per specialty (mean, std)
SPECIALTY_COST_PARAMS: Dict[str, tuple] = {
    "Cardiology":           (3200, 1800),
    "Orthopedic Surgery":   (4500, 2500),
    "Oncology":             (5200, 3000),
    "Psychiatry":           (800,  400),
    "Family Medicine":      (350,  200),
    "Internal Medicine":    (550,  300),
    "Neurology":            (1800, 1000),
    "Dermatology":          (650,  350),
    "Gastroenterology":     (2100, 1200),
    "Pulmonology":          (1500, 900),
    "Endocrinology":        (900,  500),
    "Rheumatology":         (1200, 700),
    "Urology":              (2200, 1300),
    "Ophthalmology":        (1100, 600),
    "Otolaryngology":       (1600, 900),
    "Nephrology":           (1700, 1000),
    "Hematology":           (3800, 2200),
    "Infectious Disease":   (1400, 800),
    "Geriatrics":           (700,  400),
    "Allergy/Immunology":   (750,  400),
    "Physical Medicine":    (1000, 600),
    "Radiology":            (1300, 700),
    "Pathology":            (2400, 1400),
    "Anesthesiology":       (3500, 2000),
    "Emergency Medicine":   (1800, 1100),
    "Obstetrics/Gynecology":(1200, 700),
    "Pediatrics":           (400,  220),
    "Vascular Surgery":     (3900, 2200),
    "Thoracic Surgery":     (6200, 3500),
    "Colorectal Surgery":   (4100, 2300),
}

# HCPCS procedure codes (representative sample per specialty)
PROCEDURE_CODES: Dict[str, List[tuple]] = {
    "Cardiology": [
        ("93000", "Electrocardiogram (ECG)"),
        ("93306", "Echocardiography, Complete"),
        ("93510", "Left Heart Catheterization"),
        ("92928", "Percutaneous Coronary Intervention"),
        ("93880", "Duplex Scan of Extracranial Arteries"),
        ("93017", "Cardiovascular Stress Test"),
        ("93270", "External ECG Patient Recording"),
        ("93653", "Cardiac Electrophysiology Study"),
    ],
    "Orthopedic Surgery": [
        ("27447", "Total Knee Replacement"),
        ("27130", "Total Hip Replacement"),
        ("29827", "Arthroscopy, Shoulder Rotator Cuff"),
        ("29881", "Arthroscopy, Knee with Meniscectomy"),
        ("22612", "Lumbar Spinal Fusion"),
        ("25600", "Closed Treatment Distal Radius Fracture"),
        ("27570", "Manipulation of Knee Joint"),
        ("20610", "Aspiration/Injection Major Joint"),
    ],
    "Oncology": [
        ("96413", "Chemotherapy, Infusion 1 Hour"),
        ("96415", "Chemotherapy, Infusion Additional Hour"),
        ("77067", "Screening Mammography"),
        ("77263", "Radiation Treatment Planning"),
        ("96401", "Chemotherapy, Non-Hormonal"),
        ("96411", "Chemotherapy, Injection"),
        ("77014", "CT Guidance for Needle Placement"),
        ("38221", "Bone Marrow Biopsy"),
    ],
    "Psychiatry": [
        ("90837", "Psychotherapy 60 Minutes"),
        ("90834", "Psychotherapy 45 Minutes"),
        ("90792", "Psychiatric Diagnostic Evaluation"),
        ("99213", "Office Visit, Established Patient"),
        ("90853", "Group Psychotherapy"),
        ("96116", "Neurobehavioral Status Exam"),
        ("90867", "Therapeutic TMS Treatment"),
        ("90839", "Psychotherapy for Crisis"),
    ],
    "Family Medicine": [
        ("99213", "Office Visit, Established Low-Moderate"),
        ("99214", "Office Visit, Established Moderate"),
        ("99203", "Office Visit, New Patient Moderate"),
        ("99395", "Preventive Visit, Established 18-39"),
        ("90718", "Td Vaccine Injection"),
        ("93000", "Electrocardiogram"),
        ("82947", "Glucose Blood Test"),
        ("85025", "Complete Blood Count"),
    ],
    "Internal Medicine": [
        ("99214", "Office Visit, Established Moderate"),
        ("99215", "Office Visit, Established High"),
        ("99232", "Subsequent Hospital Care"),
        ("99291", "Critical Care First 30-74 Min"),
        ("36415", "Blood Draw Venipuncture"),
        ("93000", "Electrocardiogram"),
        ("71046", "Chest X-Ray 2 Views"),
        ("85025", "Complete Blood Count"),
    ],
    "Neurology": [
        ("95819", "EEG with Activation Procedures"),
        ("95886", "Needle EMG, each extremity"),
        ("93880", "Duplex Scan Extracranial Arteries"),
        ("99214", "Office Visit, Established Moderate"),
        ("95920", "Intraoperative Neurophysiology"),
        ("95930", "Visual Evoked Potential"),
        ("95907", "Nerve Conduction Studies 1-2"),
        ("96020", "Neurofunctional Testing"),
    ],
    "Dermatology": [
        ("11440", "Excision Benign Lesion 0.5cm"),
        ("17000", "Destruction Premalignant Lesion"),
        ("11100", "Skin Biopsy Single Lesion"),
        ("99213", "Office Visit, Established Low-Moderate"),
        ("96920", "Laser Treatment Inflammatory Skin"),
        ("17110", "Destruction Warts up to 14"),
        ("11602", "Excision Malignant Lesion"),
        ("96931", "Reflectance Confocal Microscopy"),
    ],
    "Gastroenterology": [
        ("45378", "Colonoscopy, Diagnostic"),
        ("43239", "EGD with Biopsy"),
        ("45380", "Colonoscopy with Biopsy"),
        ("43235", "EGD, Diagnostic"),
        ("43239", "Upper GI Endoscopy Biopsy"),
        ("45385", "Colonoscopy with Polyp Removal"),
        ("91022", "Duodenal Motility Study"),
        ("43246", "EGD with Directed Submucosal Injection"),
    ],
    "Pulmonology": [
        ("94010", "Spirometry"),
        ("94060", "Spirometry with Bronchodilator"),
        ("31622", "Bronchoscopy, Diagnostic"),
        ("94640", "Pressurized Inhalation Treatment"),
        ("94664", "Aerosol or Vapor Inhalation Demonstration"),
        ("94726", "Plethysmography"),
        ("94750", "Pulmonary Compliance Study"),
        ("31641", "Bronchoscopy, Dilation"),
    ],
    "Endocrinology": [
        ("99213", "Office Visit, Established Low-Moderate"),
        ("99214", "Office Visit, Established Moderate"),
        ("83036", "Hemoglobin A1C"),
        ("76536", "Ultrasound Head and Neck"),
        ("77080", "DEXA Scan Bone Density"),
        ("60210", "Partial Thyroidectomy"),
        ("82306", "Vitamin D 25-Hydroxy"),
        ("83001", "Gonadotropin Follicle Stimulating"),
    ],
    "Rheumatology": [
        ("99214", "Office Visit, Established Moderate"),
        ("20610", "Joint Aspiration/Injection"),
        ("20600", "Small Joint Aspiration"),
        ("96413", "Biologic Infusion Therapy"),
        ("86235", "ANA Antibody Test"),
        ("86200", "Cyclic Citrullinated Antibody"),
        ("77080", "DEXA Scan"),
        ("20552", "Trigger Point Injection"),
    ],
    "Urology": [
        ("52601", "Transurethral Prostatectomy"),
        ("52310", "Cystoscopy with Removal Foreign Body"),
        ("55873", "Cryosurgical Ablation Prostate"),
        ("50590", "Lithotripsy Extracorporeal Shock Wave"),
        ("51700", "Bladder Irrigation Simple"),
        ("55700", "Prostate Biopsy"),
        ("53600", "Dilation Male Urethra"),
        ("76872", "Ultrasound Transrectal"),
    ],
    "Ophthalmology": [
        ("66984", "Cataract Surgery"),
        ("67028", "Intravitreal Injection"),
        ("92012", "Eye Exam Established Patient"),
        ("92132", "Scanning Computerized Ophthalmic"),
        ("92083", "Visual Field Examination"),
        ("92133", "Optic Disc Imaging"),
        ("67210", "Photocoagulation Macular"),
        ("92285", "Fundus Photography"),
    ],
    "Otolaryngology": [
        ("42821", "Tonsillectomy and Adenoidectomy"),
        ("69436", "Tympanostomy General Anesthesia"),
        ("31255", "Nasal/Sinus Endoscopy with Ethmoidectomy"),
        ("92557", "Comprehensive Audiometry"),
        ("30520", "Septoplasty"),
        ("21385", "Open Treatment Orbital Floor Fracture"),
        ("42830", "Adenoidectomy Primary under 12"),
        ("69711", "Implantation Cochlear Device"),
    ],
    "Nephrology": [
        ("90935", "Hemodialysis One Evaluation"),
        ("90945", "Peritoneal Dialysis One Evaluation"),
        ("50300", "Kidney Transplant Cadaver"),
        ("99213", "Office Visit, Established Low-Moderate"),
        ("90951", "ESRD Services Monthly 2-11 years"),
        ("90960", "ESRD Services Monthly 20+ years"),
        ("50590", "Lithotripsy"),
        ("74178", "CT Abdomen/Pelvis with Contrast"),
    ],
    "Hematology": [
        ("38220", "Bone Marrow Aspiration"),
        ("38221", "Bone Marrow Biopsy"),
        ("96413", "Chemotherapy Infusion 1 Hour"),
        ("36430", "Blood Transfusion"),
        ("85025", "Complete Blood Count"),
        ("85610", "Prothrombin Time"),
        ("86355", "B Cells Total Count"),
        ("88305", "Surgical Pathology Examination"),
    ],
    "Infectious Disease": [
        ("99214", "Office Visit, Established Moderate"),
        ("86703", "HIV-1/HIV-2 Antibody Test"),
        ("87621", "Chlamydia Amplified Probe"),
        ("87591", "Gonorrhea Amplified Probe"),
        ("86803", "Hepatitis C Antibody"),
        ("86592", "Syphilis Test Non-Treponemal Qual"),
        ("87635", "Infectious Agent Detection"),
        ("90633", "Hepatitis A Vaccine"),
    ],
    "Geriatrics": [
        ("99213", "Office Visit, Established Low-Moderate"),
        ("99214", "Office Visit, Established Moderate"),
        ("99341", "Home Visit, New Patient"),
        ("99347", "Home Visit, Established Patient"),
        ("96125", "Standardized Cognitive Performance Testing"),
        ("99483", "Assessment/Care Planning for Cognitive Impairment"),
        ("99366", "Medical Team Conference"),
        ("96160", "Health Risk Assessment Instrument"),
    ],
    "Allergy/Immunology": [
        ("95004", "Percutaneous Allergy Tests"),
        ("95024", "Intracutaneous Allergy Tests Sequential"),
        ("95165", "Antigen Therapy Services"),
        ("95115", "Immunotherapy Injection Single"),
        ("99213", "Office Visit, Established Low-Moderate"),
        ("86003", "Allergen Specific IgE"),
        ("94010", "Spirometry"),
        ("95068", "Allergy Immunotherapy Stinging Insects"),
    ],
    "Physical Medicine": [
        ("97110", "Therapeutic Exercises"),
        ("97014", "Electrical Stimulation"),
        ("97012", "Traction Mechanical"),
        ("97530", "Therapeutic Activities"),
        ("97010", "Application of Hot or Cold Pack"),
        ("97035", "Ultrasound Treatment"),
        ("20610", "Joint Aspiration"),
        ("64483", "Injection Anesthetic/Steroid Lumbar"),
    ],
    "Radiology": [
        ("71046", "Chest X-Ray 2 Views"),
        ("74178", "CT Abdomen/Pelvis with Contrast"),
        ("70553", "MRI Brain without/with Contrast"),
        ("73721", "MRI Joint Lower Extremity"),
        ("77067", "Screening Mammography"),
        ("72148", "MRI Lumbar Spine"),
        ("74177", "CT Abdomen/Pelvis with Contrast"),
        ("76700", "Ultrasound Abdomen Complete"),
    ],
    "Pathology": [
        ("88305", "Surgical Pathology Level IV"),
        ("88307", "Surgical Pathology Level V"),
        ("88342", "Immunohistochemistry per Antibody"),
        ("88360", "Morphometric Analysis Tumor"),
        ("88182", "Flow Cytometry Each Cell Surface"),
        ("88271", "Molecular Cytogenetics DNA Probe"),
        ("88302", "Surgical Pathology Level II"),
        ("88323", "Consultation Review Slides"),
    ],
    "Anesthesiology": [
        ("00840", "Anesthesia Intraperitoneal Procedures"),
        ("00630", "Anesthesia Lumbar Region"),
        ("00790", "Anesthesia Upper Abdomen"),
        ("01610", "Anesthesia Shoulder Joint Procedures"),
        ("64450", "Injection Neural Blockade"),
        ("62323", "Injection Lumbar Epidural Steroid"),
        ("64415", "Injection Brachial Plexus Nerve Block"),
        ("62322", "Injection Lumbar Epidural Diagnostic"),
    ],
    "Emergency Medicine": [
        ("99283", "ED Visit Moderate Severity"),
        ("99284", "ED Visit Moderate-High Severity"),
        ("99285", "ED Visit High Severity"),
        ("99282", "ED Visit Low-Moderate Severity"),
        ("12001", "Simple Repair Superficial Wounds"),
        ("36415", "Blood Draw"),
        ("71046", "Chest X-Ray"),
        ("99291", "Critical Care First Hour"),
    ],
    "Obstetrics/Gynecology": [
        ("59400", "Routine Obstetric Care Vaginal Delivery"),
        ("59510", "Routine Obstetric Care Cesarean Delivery"),
        ("58150", "Total Abdominal Hysterectomy"),
        ("57454", "Colposcopy with Biopsy"),
        ("76805", "Ultrasound Pregnant Uterus"),
        ("99213", "Office Visit, Established Low-Moderate"),
        ("59025", "Fetal Non-Stress Test"),
        ("58661", "Laparoscopy Removal Adnexal Structures"),
    ],
    "Pediatrics": [
        ("99393", "Preventive Visit Established 5-11 Years"),
        ("99392", "Preventive Visit Established 1-4 Years"),
        ("99394", "Preventive Visit Established 12-17 Years"),
        ("99213", "Office Visit, Established Low-Moderate"),
        ("90703", "Tetanus Toxoid Vaccine"),
        ("90633", "Hepatitis A Vaccine"),
        ("99381", "Preventive Visit New Infant"),
        ("90471", "Immunization Administration"),
    ],
    "Vascular Surgery": [
        ("35301", "Carotid Endarterectomy"),
        ("33880", "Endovascular Thoracic Aortic Repair"),
        ("35656", "Bypass Graft Femoral-Popliteal"),
        ("37228", "Tibial/Peroneal Artery Angioplasty"),
        ("36247", "Catheterization Selective Visceral"),
        ("75716", "Angiography Extremity Bilateral"),
        ("34812", "Open Femoral Artery Exposure"),
        ("35371", "Thromboendarterectomy Femoral Artery"),
    ],
    "Thoracic Surgery": [
        ("32663", "Thoracoscopy Lobectomy"),
        ("32480", "Removal of Lung, Single Lobe"),
        ("33533", "CABG Arterial Single"),
        ("33534", "CABG Arterial Two"),
        ("33510", "CABG Venous Single"),
        ("32310", "Pleurectomy Partial"),
        ("32654", "Thoracoscopy Resection"),
        ("33250", "Operative Ablation Supraventricular"),
    ],
    "Colorectal Surgery": [
        ("44140", "Colectomy Partial with Anastomosis"),
        ("45110", "Abdominoperineal Resection"),
        ("45121", "Proctectomy Combined"),
        ("44145", "Colectomy Partial with Low Anterior"),
        ("46260", "Hemorrhoidectomy Internal and External"),
        ("45380", "Colonoscopy with Biopsy"),
        ("46750", "Sphincteroplasty Adult"),
        ("44187", "Laparoscopy Ileostomy"),
    ],
}

CITIES_BY_STATE = {
    "CA": ["Los Angeles", "San Francisco", "San Diego", "Sacramento", "Fresno"],
    "TX": ["Houston", "Dallas", "San Antonio", "Austin", "Fort Worth"],
    "FL": ["Miami", "Orlando", "Tampa", "Jacksonville", "Fort Lauderdale"],
    "NY": ["New York", "Buffalo", "Rochester", "Albany", "Syracuse"],
    "PA": ["Philadelphia", "Pittsburgh", "Allentown", "Erie", "Reading"],
    "IL": ["Chicago", "Rockford", "Aurora", "Joliet", "Naperville"],
    "OH": ["Columbus", "Cleveland", "Cincinnati", "Toledo", "Akron"],
    "GA": ["Atlanta", "Augusta", "Columbus", "Macon", "Savannah"],
    "NC": ["Charlotte", "Raleigh", "Greensboro", "Durham", "Winston-Salem"],
    "MI": ["Detroit", "Grand Rapids", "Warren", "Sterling Heights", "Lansing"],
    "NJ": ["Newark", "Jersey City", "Paterson", "Elizabeth", "Trenton"],
    "VA": ["Virginia Beach", "Norfolk", "Chesapeake", "Richmond", "Newport News"],
    "WA": ["Seattle", "Spokane", "Tacoma", "Vancouver", "Bellevue"],
    "AZ": ["Phoenix", "Tucson", "Mesa", "Chandler", "Scottsdale"],
    "TN": ["Nashville", "Memphis", "Knoxville", "Chattanooga", "Clarksville"],
    "MA": ["Boston", "Worcester", "Springfield", "Lowell", "Cambridge"],
    "IN": ["Indianapolis", "Fort Wayne", "Evansville", "South Bend", "Carmel"],
    "MO": ["Kansas City", "St. Louis", "Springfield", "Columbia", "Independence"],
    "MD": ["Baltimore", "Frederick", "Rockville", "Gaithersburg", "Bowie"],
    "WI": ["Milwaukee", "Madison", "Green Bay", "Kenosha", "Racine"],
    "CO": ["Denver", "Colorado Springs", "Aurora", "Fort Collins", "Lakewood"],
    "MN": ["Minneapolis", "Saint Paul", "Rochester", "Duluth", "Brooklyn Park"],
    "SC": ["Columbia", "Charleston", "North Charleston", "Mount Pleasant", "Rock Hill"],
    "AL": ["Birmingham", "Montgomery", "Huntsville", "Mobile", "Tuscaloosa"],
    "LA": ["New Orleans", "Baton Rouge", "Shreveport", "Lafayette", "Lake Charles"],
    "KY": ["Louisville", "Lexington", "Bowling Green", "Owensboro", "Covington"],
    "OR": ["Portland", "Salem", "Eugene", "Gresham", "Hillsboro"],
    "OK": ["Oklahoma City", "Tulsa", "Norman", "Broken Arrow", "Lawton"],
    "CT": ["Bridgeport", "New Haven", "Hartford", "Stamford", "Waterbury"],
    "UT": ["Salt Lake City", "West Valley City", "Provo", "West Jordan", "Orem"],
    "IA": ["Des Moines", "Cedar Rapids", "Davenport", "Sioux City", "Iowa City"],
    "NV": ["Las Vegas", "Henderson", "Reno", "North Las Vegas", "Sparks"],
    "AR": ["Little Rock", "Fort Smith", "Fayetteville", "Springdale", "Jonesboro"],
    "MS": ["Jackson", "Gulfport", "Southaven", "Hattiesburg", "Biloxi"],
    "KS": ["Wichita", "Overland Park", "Kansas City", "Topeka", "Olathe"],
    "NM": ["Albuquerque", "Las Cruces", "Rio Rancho", "Santa Fe", "Roswell"],
    "NE": ["Omaha", "Lincoln", "Bellevue", "Grand Island", "Kearney"],
    "WV": ["Charleston", "Huntington", "Morgantown", "Parkersburg", "Wheeling"],
    "ID": ["Boise", "Meridian", "Nampa", "Idaho Falls", "Pocatello"],
    "HI": ["Honolulu", "Pearl City", "Hilo", "Kailua", "Waipahu"],
    "NH": ["Manchester", "Nashua", "Concord", "Derry", "Dover"],
    "ME": ["Portland", "Lewiston", "Bangor", "South Portland", "Auburn"],
    "RI": ["Providence", "Cranston", "Warwick", "Pawtucket", "East Providence"],
    "MT": ["Billings", "Missoula", "Great Falls", "Bozeman", "Butte"],
    "DE": ["Wilmington", "Dover", "Newark", "Middletown", "Smyrna"],
    "SD": ["Sioux Falls", "Rapid City", "Aberdeen", "Brookings", "Watertown"],
    "ND": ["Fargo", "Bismarck", "Grand Forks", "Minot", "West Fargo"],
    "AK": ["Anchorage", "Fairbanks", "Juneau", "Sitka", "Ketchikan"],
    "VT": ["Burlington", "South Burlington", "Rutland", "Barre", "Montpelier"],
    "WY": ["Cheyenne", "Casper", "Laramie", "Gillette", "Rock Springs"],
}

# Regional cost multipliers
REGION_MULTIPLIERS = {
    "Northeast": 1.20,
    "Southeast": 0.95,
    "Midwest": 0.90,
    "Southwest": 1.05,
    "West": 1.15,
}

STATE_REGION = {
    "CT": "Northeast", "ME": "Northeast", "MA": "Northeast", "NH": "Northeast",
    "NJ": "Northeast", "NY": "Northeast", "PA": "Northeast", "RI": "Northeast",
    "VT": "Northeast",
    "AL": "Southeast", "AR": "Southeast", "DE": "Southeast", "FL": "Southeast",
    "GA": "Southeast", "KY": "Southeast", "LA": "Southeast", "MD": "Southeast",
    "MS": "Southeast", "NC": "Southeast", "SC": "Southeast", "TN": "Southeast",
    "VA": "Southeast", "WV": "Southeast",
    "IL": "Midwest", "IN": "Midwest", "IA": "Midwest", "KS": "Midwest",
    "MI": "Midwest", "MN": "Midwest", "MO": "Midwest", "NE": "Midwest",
    "ND": "Midwest", "OH": "Midwest", "SD": "Midwest", "WI": "Midwest",
    "AZ": "Southwest", "NM": "Southwest", "OK": "Southwest", "TX": "Southwest",
    "AK": "West", "CA": "West", "CO": "West", "HI": "West", "ID": "West",
    "MT": "West", "NV": "West", "OR": "West", "UT": "West", "WA": "West",
    "WY": "West",
}

FIRST_NAMES = [
    "James", "John", "Robert", "Michael", "William", "David", "Richard", "Joseph",
    "Thomas", "Charles", "Mary", "Patricia", "Jennifer", "Linda", "Barbara",
    "Elizabeth", "Susan", "Jessica", "Sarah", "Karen", "Lisa", "Nancy", "Betty",
    "Margaret", "Sandra", "Ashley", "Emily", "Donna", "Michelle", "Dorothy",
    "Daniel", "Mark", "Paul", "Donald", "Kenneth", "Steven", "Kevin", "Brian",
    "George", "Timothy", "Helen", "Deborah", "Rachel", "Carolyn", "Janet",
    "Maria", "Sharon", "Laura", "Amy", "Kimberly"
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson",
    "Walker", "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen",
    "Hill", "Flores", "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera",
    "Campbell", "Mitchell", "Carter", "Roberts"
]


def generate_npi() -> str:
    """Generate a realistic 10-digit NPI number."""
    return str(random.randint(1000000000, 1999999999))


def generate_synthetic_data(n_rows: int = 100_000) -> pd.DataFrame:
    """
    Generate a synthetic Medicare provider dataset.

    Args:
        n_rows: Target number of rows to generate.

    Returns:
        DataFrame with Medicare provider data.
    """
    print(f"Generating {n_rows:,} rows of synthetic Medicare data...")

    # Build state weights list
    states_list = list(STATE_WEIGHTS.keys())
    state_probs = np.array([STATE_WEIGHTS[s] for s in states_list], dtype=float)
    state_probs /= state_probs.sum()

    rows = []
    used_npis = set()

    # Determine rows per specialty (roughly equal distribution)
    rows_per_specialty = n_rows // len(SPECIALTIES)

    for specialty in SPECIALTIES:
        cost_mean, cost_std = SPECIALTY_COST_PARAMS[specialty]
        procedures = PROCEDURE_CODES.get(
            specialty,
            [("99213", "Office Visit"), ("99214", "Office Visit Detailed")]
        )

        for _ in range(rows_per_specialty):
            # Provider identity
            npi = generate_npi()
            while npi in used_npis:
                npi = generate_npi()
            used_npis.add(npi)

            first_name = random.choice(FIRST_NAMES)
            last_name = random.choice(LAST_NAMES)

            # Geography
            state = np.random.choice(states_list, p=state_probs)
            region = STATE_REGION.get(state, "Midwest")
            cities = CITIES_BY_STATE.get(state, ["City"])
            city = random.choice(cities)
            zip_code = str(random.randint(10000, 99999))

            # Choose a procedure
            hcpcs_code, hcpcs_description = random.choice(procedures)

            # Volume
            total_beneficiaries = max(1, int(np.random.lognormal(4.5, 1.2)))
            total_beneficiaries = min(total_beneficiaries, 5000)
            total_services = int(total_beneficiaries * np.random.uniform(1.1, 3.5))

            # Age
            avg_beneficiary_age = round(np.random.normal(68, 8), 1)
            avg_beneficiary_age = max(40, min(95, avg_beneficiary_age))

            # Cost with regional adjustment + some provider-level noise
            region_mult = REGION_MULTIPLIERS.get(region, 1.0)
            provider_noise = np.random.lognormal(0, 0.3)  # provider-level variation
            base_cost = max(10, np.random.normal(cost_mean, cost_std))
            avg_medicare_payment = round(base_cost * region_mult * provider_noise, 2)
            avg_medicare_payment = max(10.0, avg_medicare_payment)

            # Submitted charge is always higher than payment
            charge_multiplier = np.random.uniform(1.8, 4.5)
            avg_submitted_charge = round(avg_medicare_payment * charge_multiplier, 2)

            rows.append({
                "npi": npi,
                "provider_last_name": last_name,
                "provider_first_name": first_name,
                "provider_specialty": specialty,
                "provider_state": state,
                "provider_city": city,
                "provider_zip": zip_code,
                "hcpcs_code": hcpcs_code,
                "hcpcs_description": hcpcs_description,
                "total_beneficiaries": total_beneficiaries,
                "total_services": total_services,
                "avg_submitted_charge": avg_submitted_charge,
                "avg_medicare_payment": avg_medicare_payment,
                "avg_beneficiary_age": avg_beneficiary_age,
            })

    df = pd.DataFrame(rows)
    # Fill up to n_rows if needed due to integer division
    extra = n_rows - len(df)
    if extra > 0:
        df = pd.concat([df, df.sample(extra, random_state=42)], ignore_index=True)
        # Re-assign NPIs for duplicated rows
        for i in range(len(df) - extra, len(df)):
            npi = generate_npi()
            while npi in used_npis:
                npi = generate_npi()
            used_npis.add(npi)
            df.at[i, "npi"] = npi

    df = df.sample(frac=1, random_state=42).reset_index(drop=True)
    print(f"  Generated {len(df):,} rows across {df['provider_specialty'].nunique()} specialties")
    print(f"  Unique providers: {df['npi'].nunique():,}")
    print(f"  States covered: {df['provider_state'].nunique()}")
    print(f"  Unique HCPCS codes: {df['hcpcs_code'].nunique()}")
    return df


if __name__ == "__main__":
    output_path = os.path.join("data", "raw", "medicare_providers.csv")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    df = generate_synthetic_data(n_rows=100_000)
    df.to_csv(output_path, index=False)
    print(f"\nData saved to {output_path}")
    print(f"Shape: {df.shape}")
    print("\nColumn types:")
    print(df.dtypes)
    print("\nSample rows:")
    print(df.head(3).to_string())
