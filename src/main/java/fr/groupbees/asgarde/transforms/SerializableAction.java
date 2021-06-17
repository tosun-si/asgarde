package fr.groupbees.asgarde.transforms;

import java.io.Serializable;

/**
 * Function that allows to execute an action.
 * <p>
 * No input and no output, only an action to execute.
 * <p>
 * To be used in Beam Workers, we need to make this function as {@link Serializable}.
 */
@FunctionalInterface
public interface SerializableAction extends Serializable {

    /**
     * Execute the action.
     */
    void execute();
}