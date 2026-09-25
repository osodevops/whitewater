use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AutoscalePolicy {
    pub min_nodes: usize,
    pub max_nodes: usize,
    pub scale_out_threshold: f64,
    pub scale_in_threshold: f64,
    pub scale_out_samples: u32,
    pub scale_in_samples: u32,
    pub cooldown_samples: u32,
}

impl AutoscalePolicy {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.min_nodes < 3 || self.min_nodes > self.max_nodes {
            return Err("min_nodes must be at least three and not exceed max_nodes");
        }
        if !self.scale_in_threshold.is_finite()
            || !self.scale_out_threshold.is_finite()
            || self.scale_in_threshold < 0.0
            || self.scale_out_threshold <= self.scale_in_threshold
        {
            return Err("scale thresholds must be finite and scale_out_threshold must exceed scale_in_threshold");
        }
        if self.scale_out_samples == 0 || self.scale_in_samples == 0 {
            return Err("scale sample counts must be greater than zero");
        }
        Ok(())
    }
}

impl Default for AutoscalePolicy {
    fn default() -> Self {
        Self {
            min_nodes: 3,
            max_nodes: 20,
            scale_out_threshold: 0.75,
            scale_in_threshold: 0.20,
            scale_out_samples: 6,
            scale_in_samples: 30,
            cooldown_samples: 12,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum ScaleDecision {
    Hold {
        reason: String,
    },
    ScaleOut {
        current_nodes: usize,
        target_nodes: usize,
        pressure: f64,
    },
    ScaleIn {
        current_nodes: usize,
        target_nodes: usize,
        pressure: f64,
    },
    ScaleInBlocked {
        reason: String,
        pressure: f64,
    },
}

#[derive(Clone, Debug, Default)]
pub struct AutoscaleController {
    high_samples: u32,
    low_samples: u32,
    cooldown_remaining: u32,
}

impl AutoscaleController {
    pub fn evaluate(
        &mut self,
        policy: &AutoscalePolicy,
        pressure: f64,
        current_nodes: usize,
        removable_nodes: usize,
    ) -> ScaleDecision {
        if self.cooldown_remaining > 0 {
            self.cooldown_remaining -= 1;
            self.high_samples = 0;
            self.low_samples = 0;
            return ScaleDecision::Hold {
                reason: format!("cooldown: {} samples remaining", self.cooldown_remaining),
            };
        }

        if pressure >= policy.scale_out_threshold {
            self.high_samples = self.high_samples.saturating_add(1);
            self.low_samples = 0;
            if self.high_samples < policy.scale_out_samples {
                return ScaleDecision::Hold {
                    reason: format!(
                        "high pressure must persist for {} more samples",
                        policy.scale_out_samples - self.high_samples
                    ),
                };
            }
            self.high_samples = 0;
            if current_nodes >= policy.max_nodes {
                return ScaleDecision::Hold {
                    reason: "maximum node count reached".to_owned(),
                };
            }
            self.cooldown_remaining = policy.cooldown_samples;
            return ScaleDecision::ScaleOut {
                current_nodes,
                target_nodes: current_nodes + 1,
                pressure,
            };
        }

        if pressure <= policy.scale_in_threshold {
            self.low_samples = self.low_samples.saturating_add(1);
            self.high_samples = 0;
            if self.low_samples < policy.scale_in_samples {
                return ScaleDecision::Hold {
                    reason: format!(
                        "low pressure must persist for {} more samples",
                        policy.scale_in_samples - self.low_samples
                    ),
                };
            }
            self.low_samples = 0;
            if current_nodes <= policy.min_nodes {
                return ScaleDecision::Hold {
                    reason: "minimum node count reached".to_owned(),
                };
            }
            if removable_nodes == 0 {
                return ScaleDecision::ScaleInBlocked {
                    reason: "no empty or fully drained node is safe to remove".to_owned(),
                    pressure,
                };
            }
            self.cooldown_remaining = policy.cooldown_samples;
            return ScaleDecision::ScaleIn {
                current_nodes,
                target_nodes: current_nodes - 1,
                pressure,
            };
        }

        self.high_samples = 0;
        self.low_samples = 0;
        ScaleDecision::Hold {
            reason: "pressure is inside the stable band".to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> AutoscalePolicy {
        AutoscalePolicy {
            min_nodes: 3,
            max_nodes: 6,
            scale_out_samples: 3,
            scale_in_samples: 4,
            cooldown_samples: 2,
            ..AutoscalePolicy::default()
        }
    }

    #[test]
    fn scale_out_requires_sustained_pressure_and_moves_one_node() {
        let mut controller = AutoscaleController::default();
        assert!(matches!(
            controller.evaluate(&policy(), 0.9, 3, 0),
            ScaleDecision::Hold { .. }
        ));
        assert!(matches!(
            controller.evaluate(&policy(), 0.9, 3, 0),
            ScaleDecision::Hold { .. }
        ));
        assert_eq!(
            controller.evaluate(&policy(), 0.9, 3, 0),
            ScaleDecision::ScaleOut {
                current_nodes: 3,
                target_nodes: 4,
                pressure: 0.9
            }
        );
        assert!(matches!(
            controller.evaluate(&policy(), 0.9, 4, 0),
            ScaleDecision::Hold { .. }
        ));
    }

    #[test]
    fn stable_band_resets_high_pressure_evidence() {
        let mut controller = AutoscaleController::default();
        controller.evaluate(&policy(), 0.9, 3, 0);
        controller.evaluate(&policy(), 0.5, 3, 0);
        controller.evaluate(&policy(), 0.9, 3, 0);
        assert!(matches!(
            controller.evaluate(&policy(), 0.9, 3, 0),
            ScaleDecision::Hold { .. }
        ));
    }

    #[test]
    fn scale_in_is_slow_and_requires_a_safe_removal_candidate() {
        let mut controller = AutoscaleController::default();
        for _ in 0..3 {
            controller.evaluate(&policy(), 0.1, 5, 0);
        }
        assert!(matches!(
            controller.evaluate(&policy(), 0.1, 5, 0),
            ScaleDecision::ScaleInBlocked { .. }
        ));
        for _ in 0..3 {
            controller.evaluate(&policy(), 0.1, 5, 1);
        }
        assert_eq!(
            controller.evaluate(&policy(), 0.1, 5, 1),
            ScaleDecision::ScaleIn {
                current_nodes: 5,
                target_nodes: 4,
                pressure: 0.1
            }
        );
    }
}
