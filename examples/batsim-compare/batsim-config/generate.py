import json

jobs = [
    {
        "id": f"homo_{id}",
        "subtime": 10,
        "walltime": 1000000000,
        "res": 5,
        "profile": "homogeneous"
    }
    for id in range(8000)
]

print(json.dumps(jobs, indent=4))