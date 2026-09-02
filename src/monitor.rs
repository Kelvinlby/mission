use nvml_wrapper::Nvml;
use nvml_wrapper::{enums::gpm::GpmMetricId, gpm::gpm_metrics_get};
use std::collections::{HashSet, VecDeque};
use sysinfo::{Pid, ProcessesToUpdate, System};

pub const HISTORY_LEN: usize = 180;

#[derive(Clone, Debug, Default)]
pub struct GpuSample {
    pub gpu_percent: f64,
    pub memory_percent: f64,
    pub memory_used: u64,
    pub memory_total: u64,
    pub process_memory: u64,
    pub process_gpu_percent: f64,
    pub sm_percent: Option<f64>,
    pub tensor_percent: Option<f64>,
}

#[derive(Clone, Debug, Default)]
pub struct Sample {
    pub global_cpu: f64,
    pub global_memory: f64,
    pub memory_used: u64,
    pub memory_total: u64,
    pub process_cpu: f64,
    pub process_memory: u64,
    pub process_count: usize,
    pub gpus: Vec<GpuSample>,
}

#[derive(Default)]
pub struct Histories {
    pub global_cpu: VecDeque<f64>,
    pub global_memory: VecDeque<f64>,
    pub process_cpu: VecDeque<f64>,
    pub process_memory: VecDeque<f64>,
    pub gpu: Vec<VecDeque<f64>>,
    pub vram: Vec<VecDeque<f64>>,
    pub sm: Vec<VecDeque<f64>>,
    pub tensor: Vec<VecDeque<f64>>,
}

impl Histories {
    pub fn push(&mut self, sample: &Sample) {
        push(&mut self.global_cpu, sample.global_cpu);
        push(&mut self.global_memory, sample.global_memory);
        push(&mut self.process_cpu, sample.process_cpu);
        let process_mem_percent = if sample.memory_total == 0 {
            0.0
        } else {
            sample.process_memory as f64 * 100.0 / sample.memory_total as f64
        };
        push(&mut self.process_memory, process_mem_percent);
        while self.gpu.len() < sample.gpus.len() {
            self.gpu.push(VecDeque::new());
            self.vram.push(VecDeque::new());
            self.sm.push(VecDeque::new());
            self.tensor.push(VecDeque::new());
        }
        for (index, gpu) in sample.gpus.iter().enumerate() {
            push(&mut self.gpu[index], gpu.gpu_percent);
            push(&mut self.vram[index], gpu.memory_percent);
            push(&mut self.sm[index], gpu.sm_percent.unwrap_or(0.0));
            push(&mut self.tensor[index], gpu.tensor_percent.unwrap_or(0.0));
        }
    }
}

fn push(history: &mut VecDeque<f64>, value: f64) {
    history.push_back(value.max(0.0));
    while history.len() > HISTORY_LEN {
        history.pop_front();
    }
}

pub struct Monitor {
    system: System,
    nvml: Option<Nvml>,
    root_pid: u32,
    gpu_timestamps: Vec<u64>,
}

impl Monitor {
    pub fn new(root_pid: u32) -> Self {
        Self {
            system: System::new_all(),
            nvml: Nvml::init().ok(),
            root_pid,
            gpu_timestamps: Vec::new(),
        }
    }

    pub fn sample(&mut self) -> Sample {
        self.system.refresh_cpu_usage();
        self.system.refresh_memory();
        self.system.refresh_processes(ProcessesToUpdate::All, true);
        let memory_total = self.system.total_memory();
        let memory_used = self.system.used_memory();
        let descendants = descendants(&self.system, Pid::from_u32(self.root_pid));
        let mut process_cpu = 0.0;
        let mut process_memory = 0;
        for pid in &descendants {
            if let Some(process) = self.system.process(*pid) {
                process_cpu += process.cpu_usage() as f64;
                process_memory += process.memory();
            }
        }
        let gpus = self
            .nvml
            .as_ref()
            .map(|nvml| gpu_samples(nvml, &descendants, &mut self.gpu_timestamps))
            .unwrap_or_default();
        Sample {
            global_cpu: self.system.global_cpu_usage() as f64,
            global_memory: percent(memory_used, memory_total),
            memory_used,
            memory_total,
            process_cpu,
            process_memory,
            process_count: descendants.len(),
            gpus,
        }
    }
}

fn descendants(system: &System, root: Pid) -> HashSet<Pid> {
    let mut found = if system.process(root).is_some() {
        HashSet::from([root])
    } else {
        HashSet::new()
    };
    loop {
        let before = found.len();
        for (pid, process) in system.processes() {
            if process
                .parent()
                .is_some_and(|parent| found.contains(&parent))
            {
                found.insert(*pid);
            }
        }
        if found.len() == before {
            return found;
        }
    }
}

fn gpu_samples(
    nvml: &Nvml,
    process_ids: &HashSet<Pid>,
    timestamps: &mut Vec<u64>,
) -> Vec<GpuSample> {
    let mut result = Vec::new();
    let count = nvml.device_count().unwrap_or(0);
    for index in 0..count {
        while timestamps.len() <= index as usize {
            timestamps.push(0);
        }
        let Ok(device) = nvml.device_by_index(index) else {
            continue;
        };
        let Ok(memory) = device.memory_info() else {
            continue;
        };
        let utilization = device.utilization_rates().ok();
        let process_memory = device
            .running_compute_processes()
            .unwrap_or_default()
            .into_iter()
            .filter(|process| process_ids.contains(&Pid::from_u32(process.pid)))
            .filter_map(|process| match process.used_gpu_memory {
                nvml_wrapper::enums::device::UsedGpuMemory::Used(bytes) => Some(bytes),
                nvml_wrapper::enums::device::UsedGpuMemory::Unavailable => None,
            })
            .sum();
        let mut process_gpu_percent = 0.0;
        if let Ok(samples) = device.process_utilization_stats(timestamps[index as usize]) {
            for sample in samples {
                timestamps[index as usize] = timestamps[index as usize].max(sample.timestamp);
                if process_ids.contains(&Pid::from_u32(sample.pid)) {
                    process_gpu_percent += sample.sm_util as f64;
                }
            }
        }
        let (sm_percent, tensor_percent) = gpm_utilization(nvml, &device);
        result.push(GpuSample {
            gpu_percent: utilization.as_ref().map_or(0.0, |u| u.gpu as f64),
            memory_percent: percent(memory.used, memory.total),
            memory_used: memory.used,
            memory_total: memory.total,
            process_memory,
            process_gpu_percent: process_gpu_percent.min(100.0),
            sm_percent,
            tensor_percent,
        });
    }
    result
}

fn gpm_utilization(nvml: &Nvml, device: &nvml_wrapper::Device<'_>) -> (Option<f64>, Option<f64>) {
    if !device.gpm_support().unwrap_or(false) {
        return (None, None);
    }
    let Ok(first) = device.gpm_sample() else {
        return (None, None);
    };
    std::thread::sleep(std::time::Duration::from_millis(10));
    let Ok(second) = device.gpm_sample() else {
        return (None, None);
    };
    let Ok(metrics) = gpm_metrics_get(
        nvml,
        &first,
        &second,
        &[GpmMetricId::SmUtil, GpmMetricId::AnyTensorUtil],
    ) else {
        return (None, None);
    };
    let mut values = metrics
        .into_iter()
        .map(|result| result.ok().map(|metric| metric.value));
    (values.next().flatten(), values.next().flatten())
}

fn percent(used: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        used as f64 * 100.0 / total as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn history_is_bounded_without_clipping_multicore_cpu() {
        let mut history = VecDeque::new();
        for _ in 0..HISTORY_LEN + 20 {
            push(&mut history, 150.0);
        }
        assert_eq!(history.len(), HISTORY_LEN);
        assert!(history.iter().all(|value| *value == 150.0));
    }
}
