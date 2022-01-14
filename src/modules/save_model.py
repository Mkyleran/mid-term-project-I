import pickle  # Python object serialization

def jar(model, name: 'str'):
    """
    Saves an ML model to the models directory as a pickle file
    
    Parameters
    ----------
    model : ML model fit to training data
    name : string
        Name of pickle file
    
    Returns
    -------
    None
    """
    pickle.dump(model, open(f'models/{name}.pickle.dat', 'wb'))
    return

if __name__ == '__main__':
    pass