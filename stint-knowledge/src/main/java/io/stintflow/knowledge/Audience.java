package io.stintflow.knowledge;

import io.stintflow.spi.ModelProvider;
import io.stintflow.spi.Sensitivity;

/**
 * SDD 3b.1, 4.4: the highest level a retrieval may return, from the level of the instance asking and the model the answer
 * goes to. The same rule as the provider policy of SDD 3a.5: a restricted instance reaches only a model that stays on the
 * worker, a confidential one a remote model that is approved, and a remote model that is not approved gets only what is
 * internal. The ceiling never exceeds the instance's own level, so a retrieval cannot raise a lower instance.
 */
public final class Audience {

    private Audience() {
    }

    /**
     * @param instanceLevel  the level of the instance asking, or {@code null} for none (read as restricted)
     * @param provider       the model the context is for, or {@code null} when it goes to none (read as a model that stays)
     * @param approvedRemote whether {@code provider}, if remote, is on the approved list
     */
    public static Sensitivity ceiling(Sensitivity instanceLevel, ModelProvider provider, boolean approvedRemote) {
        Sensitivity level = instanceLevel == null ? Sensitivity.RESTRICTED : instanceLevel;
        Sensitivity ofProvider;
        if (provider == null || !provider.remote()) {
            ofProvider = Sensitivity.RESTRICTED;
        } else {
            ofProvider = approvedRemote ? Sensitivity.CONFIDENTIAL : Sensitivity.INTERNAL;
        }
        return level.compareTo(ofProvider) <= 0 ? level : ofProvider;
    }
}
