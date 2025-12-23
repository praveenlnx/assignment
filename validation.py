import requests
import sys
import time

BASE_URL = "http://localhost:8000"

def log(msg):
    print(f"[TEST] {msg}")

def run_tests():
    # 1. Health Check with retries
    log("Testing Health Check (waiting for ES)...")
    max_retries = 30
    for i in range(max_retries):
        try:
            r = requests.get(f"{BASE_URL}/health")
            if r.status_code == 200:
                data = r.json()
                if data.get("elasticsearch") == "connected":
                    log("Health Check OK & ES Connected")
                    break
                else:
                    log(f"Health OK but ES not connected: {data}")
            else:
                 log(f"Health Check Status: {r.status_code}")
        except Exception as e:
            pass # ignore conn errors
        
        time.sleep(2)
        if i == max_retries - 1:
            log("Timeout waiting for Health/ES")
            sys.exit(1)

    # 2. Upsert City
    log("Testing Upsert (PUT /api/population)...")
    data = {"city": "Metropolis", "population": 5000000}
    try:
        r = requests.put(f"{BASE_URL}/api/population", json=data)
        if r.status_code != 200:
            log(f"Upsert failed: {r.text}")
            sys.exit(1)
        log("Upsert OK")
    except Exception as e:
        log(f"Upsert Exception: {e}")
        sys.exit(1)

    # 3. Get City
    log("Testing Get (GET /api/population/{city})...")
    try:
        r = requests.get(f"{BASE_URL}/api/population/Metropolis")
        if r.status_code != 200:
            log(f"Get failed: {r.text}")
            sys.exit(1)
        resp_data = r.json()
        if resp_data['population'] != 5000000:
            log(f"Population mismatch: {resp_data['population']}")
            sys.exit(1)
        log("Get OK")
    except Exception as e:
        log(f"Get Exception: {e}")
        sys.exit(1)

    # 4. Update City
    log("Testing Update (Upsert existing)...")
    data['population'] = 6000000
    try:
        r = requests.put(f"{BASE_URL}/api/population", json=data)
        assert r.status_code == 200
        
        r = requests.get(f"{BASE_URL}/api/population/Metropolis")
        assert r.json()['population'] == 6000000
        log("Update OK")
    except Exception as e:
        log(f"Update Exception: {e}")
        sys.exit(1)

    log("ALL TESTS PASSED")

if __name__ == "__main__":
    run_tests()
