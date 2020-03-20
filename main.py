from deps.utils import run

def getCensusData(request):
    if request.method == "POST":
        try:
            r = request.get_json()
            bucket_name = r["bucket_name"]
            run(bucket_name=bucket_name)
            return "Success"
        except Exception as e:
            return e
