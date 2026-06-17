# Databricks notebook source
# DBTITLE 1,Test Notebook: Discover All Available EANs from Wattflow API
"""
This notebook queries the Wattflow API /connections/ endpoint to discover all available EANs.
This is a test to understand if we can automatically detect new EANs instead of maintaining hardcoded lists.
"""

# COMMAND ----------

# DBTITLE 1,Imports
import requests
import json
from datetime import datetime

# COMMAND ----------

# DBTITLE 1,Configuration
# Get API key from secrets
API_KEY = dbutils.secrets.get(scope="acme_wattflow_api_creds", key="api_key")

# API endpoint
BASE_URL = "https://wattflow.e-dataportal.nl/api/v3"
CONNECTIONS_URL = f"{BASE_URL}/connections/"

# COMMAND ----------

# DBTITLE 1,Query All Connections from Wattflow API
print(f"Querying Wattflow API: {CONNECTIONS_URL}")
print(f"Timestamp: {datetime.now().isoformat()}")
print("-" * 80)

try:
    # Make API request
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Accept": "application/json"
    }

    response = requests.get(
        CONNECTIONS_URL,
        headers=headers,
        timeout=30
    )

    # Check response status
    print(f"\nAPI Response Status: {response.status_code}")

    if response.status_code == 200:
        connections_data = response.json()

        # Print raw response structure
        print(f"\nResponse Type: {type(connections_data)}")
        print(f"\nRaw Response Preview (first 1000 chars):")
        print(json.dumps(connections_data, indent=2)[:1000])

    else:
        print(f"\nError: API returned status {response.status_code}")
        print(f"Response: {response.text}")

except Exception as e:
    print(f"\nException occurred: {type(e).__name__}")
    print(f"Error message: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Parse and Extract EANs
if response.status_code == 200:
    connections_data = response.json()

    # Determine if response is a list or has a specific structure
    if isinstance(connections_data, list):
        connections_list = connections_data
    elif isinstance(connections_data, dict):
        # Check for common pagination keys
        if "results" in connections_data:
            connections_list = connections_data["results"]
        elif "data" in connections_data:
            connections_list = connections_data["data"]
        elif "connections" in connections_data:
            connections_list = connections_data["connections"]
        else:
            print("Response structure:")
            print(f"Keys: {connections_data.keys()}")
            connections_list = []
    else:
        connections_list = []

    print(f"\nTotal connections found: {len(connections_list)}")

    # Extract EANs and metadata
    if connections_list:
        print(f"\nFirst connection structure:")
        print(json.dumps(connections_list[0], indent=2))

        # Extract all EANs
        eans = []
        for conn in connections_list:
            # Try different possible field names for EAN
            ean = conn.get("ean") or conn.get("EAN") or conn.get("ean_code") or conn.get("connection_id")
            if ean:
                eans.append({
                    "ean": ean,
                    "dap_id": conn.get("dap_id") or conn.get("id"),
                    "status": conn.get("status") or conn.get("active"),
                    "address": conn.get("address") or conn.get("location"),
                    "raw": conn  # Keep full object for reference
                })

        print(f"\n{'='*80}")
        print(f"EXTRACTED EANs: {len(eans)} total")
        print(f"{'='*80}")

        # Group by status if available
        active_eans = [e for e in eans if e.get("status") in ["active", "actief", True, 1]]
        inactive_eans = [e for e in eans if e.get("status") in ["inactive", "inactief", False, 0]]
        unknown_status = [e for e in eans if e not in active_eans and e not in inactive_eans]

        print(f"\nActive EANs: {len(active_eans)}")
        print(f"Inactive EANs: {len(inactive_eans)}")
        print(f"Unknown Status: {len(unknown_status)}")

    else:
        print("\nNo connections found in response")

# COMMAND ----------

# DBTITLE 1,Display All EANs with Details
if response.status_code == 200 and eans:
    print(f"\n{'='*80}")
    print(f"ALL EANs FROM WATTFLOW API")
    print(f"{'='*80}\n")

    for i, ean_info in enumerate(eans, 1):
        print(f"{i}. EAN: {ean_info['ean']}")
        print(f"   DAP ID: {ean_info.get('dap_id', 'N/A')}")
        print(f"   Status: {ean_info.get('status', 'N/A')}")
        print(f"   Address: {ean_info.get('address', 'N/A')}")
        print()

    # Create simple lists for comparison
    all_api_eans = [e['ean'] for e in eans]

    print(f"\n{'='*80}")
    print(f"SIMPLE EAN LIST (for easy copy/paste)")
    print(f"{'='*80}\n")
    print("API_DISCOVERED_EANS = [")
    for ean in all_api_eans:
        print(f'    "{ean}",')
    print("]")

# COMMAND ----------

# DBTITLE 1,Compare with Current Hardcoded List
# Current hardcoded active EANs from b01_wattflow_api2land.py
CURRENT_ACTIVE_EANS = [
    "871689260011945210",  # Argonautenweg 57 BIJ, Rotterdam - B&T
    "871689260012433310",  # Orionstraat 235 BIJ, 's-Gravenhage - B&T Speciale Projecten
    "871689260012411691",  # Wegastraat 67 BIJ, 's-Gravenhage - B&T Speciale Projecten
    "871687120000023324",  # Plantijnweg 32, Culemborg - Concernhuisvesting (3 meters)
    "871687120000061975",  # De Serpeling 120, Lelystad - BES
    "871687910000065475",  # De Steenbok 15, 's-Hertogenbosch - BES
    "871687910000475441",  # De Steenbok 15, 's-Hertogenbosch - BES
    "871687910000475458",  # De Steenbok 15, 's-Hertogenbosch - BES
    "871694831000416077",  # Den Hulst 102, Nieuwleusen - BES (3 meters)
    "871694831000080872",  # Den Hulst 110, Nieuwleusen - BES
    "871685900041068209",  # Elsrijkdreef 199 A, Amsterdam - BES
    "871692150000024054",  # H.J.Nederhorststraat 1, Gouda - BES (9 meters)
    "871694831000211504",  # Jeverweg 16 T/M 18, Groningen - BES (3 meters)
    "871689260013155471",  # Kilkade 39, Dordrecht - BES
    "871689276000060611",  # Kilkade 53, Dordrecht - BES (2 meters)
    "871689290602500320",  # Kilkade 53, Dordrecht - BES
    "871687120000032982",  # Marowijne 34, Apeldoorn - BES
    "871690910008547670",  # Molenstraat 60, Zwammerdam - BES
    "871690910000009350",  # Molenstraat 63, Zwammerdam - BES (2 meters)
    "871687140006918639",  # Molenstraat 63, Zwammerdam - BES
    "871687400009183091",  # Proostwetering 31, Utrecht - BES (2 meters)
    "871689260012252874",  # Von Geusaustraat 195 BIJ, Voorburg - Infra Rail
    "871687400008460148",  # Tasveld 16, Montfoort - Infra Telecom
    "871689276000030706",  # Stadionweg 23, Rotterdam - Services Nederland
    "871685920004378541",  # Dijkmeerlaan 551, Amsterdam - Wonen
    "871689260013028706",  # Euryzakade 401 CVZ, Zwijndrecht - Wonen
    "871685920003789768",  # H.J.E. Wenckebachweg 1692, Amsterdam - Wonen
    "871689260013005899",  # Hartenruststraat 6 BIJ, Rotterdam - Wonen Bouw op Maat
    "871688660012152920",  # Van Embdenstrat 2 TA, Delft - Wonen Bouw op Maat
    "871685920004381039",  # Vreeswijkpad 6, Amsterdam - Wonen Bouw op Maat
    "871687110004007611",  # Akulaan 2, Ede - Wonen
    "871685920003998443",  # Bongerdkade 32, Amsterdam - Wonen
    "871685920004053639",  # Mary van der Sluisstraat 428, Amsterdam - Wonen
    "871685920004053752",  # Zuider IJdijk 76 A, Amsterdam - Wonen
    "871685920004541433",  # G.J. Scheurleerpad 8, Amsterdam - Wonen
    "871685920004481388",  # Transvaalstraat 9, Haarlem - Wonen
]

if response.status_code == 200 and eans:
    all_api_eans = [e['ean'] for e in eans]

    # Convert to sets for comparison
    api_eans_set = set(all_api_eans)
    current_eans_set = set(CURRENT_ACTIVE_EANS)

    # Find differences
    new_in_api = api_eans_set - current_eans_set
    removed_from_api = current_eans_set - api_eans_set
    still_present = api_eans_set & current_eans_set

    print(f"\n{'='*80}")
    print(f"COMPARISON: API vs Current Hardcoded List")
    print(f"{'='*80}\n")

    print(f"Total in API: {len(api_eans_set)}")
    print(f"Total in hardcoded list: {len(current_eans_set)}")
    print(f"Still present in both: {len(still_present)}")
    print(f"\n🆕 NEW EANs in API (not in hardcoded list): {len(new_in_api)}")
    if new_in_api:
        for ean in sorted(new_in_api):
            # Find full info
            full_info = next((e for e in eans if e['ean'] == ean), None)
            if full_info:
                print(f"   - {ean}")
                print(f"     Status: {full_info.get('status', 'N/A')}")
                print(f"     Address: {full_info.get('address', 'N/A')}")

    print(f"\n🗑️  REMOVED EANs (in hardcoded list but not in API): {len(removed_from_api)}")
    if removed_from_api:
        for ean in sorted(removed_from_api):
            print(f"   - {ean}")

    print(f"\n✅ MATCHED EANs (present in both): {len(still_present)}")
    print(f"   Coverage: {len(still_present)/len(current_eans_set)*100:.1f}% of hardcoded list found in API")

# COMMAND ----------

# DBTITLE 1,Summary and Recommendations
print(f"\n{'='*80}")
print(f"SUMMARY & NEXT STEPS")
print(f"{'='*80}\n")

if response.status_code == 200:
    print("✅ SUCCESS: Successfully queried Wattflow API /connections/ endpoint")
    print(f"\n📊 FINDINGS:")
    print(f"   - API returned {len(connections_list)} connections")
    print(f"   - Extracted {len(eans)} EANs with metadata")

    if new_in_api:
        print(f"\n⚠️  ACTION REQUIRED:")
        print(f"   - {len(new_in_api)} new EAN(s) found in API that are NOT in current pipeline")
        print(f"   - Review these EANs and update the hardcoded list if needed")

    if removed_from_api:
        print(f"\n⚠️  POTENTIAL ISSUE:")
        print(f"   - {len(removed_from_api)} EAN(s) in hardcoded list NOT found in API")
        print(f"   - These may have been decommissioned or have restricted access")

    print(f"\n💡 RECOMMENDATION:")
    print(f"   - Implement automatic EAN discovery to eliminate manual maintenance")
    print(f"   - Use this endpoint to dynamically fetch available EANs before processing")
    print(f"   - Add alerting when new EANs appear or existing ones disappear")

else:
    print("❌ FAILED: Could not query Wattflow API")
    print(f"   - Status Code: {response.status_code}")
    print(f"   - Check API credentials and endpoint availability")

print(f"\n{'='*80}")
