def __getattr__(name: str):
    if name == "GeodeticGatekeeper":
        from totali.geodetic.gatekeeper import GeodeticGatekeeper
        return GeodeticGatekeeper
    if name == "CRSInferenceEngine":
        from totali.geodetic.crs_inference import CRSInferenceEngine
        return CRSInferenceEngine
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ["GeodeticGatekeeper", "CRSInferenceEngine"]
