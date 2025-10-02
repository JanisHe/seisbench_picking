"""
Load picker models that are integrated in SeisBench.
Add new models by extending the function.
Note, in a first step the model is loaded using the load method, afterwards the model is loaded from
pretrained models. If not model is found, SeisBench returns an error message.
"""

import seisbench.models as sbm  # noqa


def get_picker(type: str, model_name: str):
    """
    Load picker from SeisBench.
    If picker or model is not available, an error is raised.
    Note pickers can be easily added by extending the code.

    :param type: Neural network of SeisBench picker
    :param model_name: Name or filename of pretrained weights for picker
    """
    if type.lower() == "phasenet":
        try:
            picker = sbm.PhaseNet.load(model_name)
        except FileNotFoundError:
            picker = sbm.PhaseNet.from_pretrained(model_name)
    elif type.lower() == "eqt":
        try:
            picker = sbm.EQTransformer.load(model_name)
        except FileNotFoundError:
            picker = sbm.EQTransformer.from_pretrained(model_name)
    elif type.lower() == "gpd":
        try:
            picker = sbm.GPD.load(model_name)
        except FileNotFoundError:
            picker = sbm.GPD.from_pretrained(model_name)
    else:
        msg = f"SeisBench model {type} is not implemented in core.picking_interfaces.py"
        raise ValueError(msg)

    return picker
