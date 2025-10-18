module Pyroscope

using Profile, PProf, HTTP, Dates
using Base.Threads: @spawn, sleep

export start_profiling, example

# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

mutable struct ProfilerConfig
    interval::Float64
    duration::Float64
    enabled_cpu::Bool
    enabled_alloc::Bool
end

const DEFAULT_CONFIG = ProfilerConfig(60.0, 5.0, true, true)

function collect_pprof_snapshot(kind::Symbol; duration::Real=5.0, outpath::AbstractString="profile.pb.gz")
    t_start = Dates.now()
    if kind == :cpu
        Profile.clear()
        @profile sleep(duration)

        PProf.pprof(; web=false, out=outpath)
        data = Profile.fetch()
    elseif kind == :allocs
        Profile.Allocs.clear()
        Profile.Allocs.@profile sample_rate = 0.01 sleep(duration)
        PProf.Allocs.pprof(; web=false, out=outpath)
    else
        error("Unknown profile kind: $kind")
    end
    t_end = Dates.now()

    return outpath, t_start, t_end
end


function upload_pprof(outpath::AbstractString; t_start::DateTime, t_end::DateTime, profile_type::String="cpu")
    server = get(ENV, "PYROSCOPE_SERVER", "http://localhost:4040")
    app = get(ENV, "PYROSCOPE_APP", "julia.app")
    from_unix = Dates.datetime2unix(t_start)
    until_unix = Dates.datetime2unix(t_end)
    url = string(server, "/ingest?format=pprof&name=", app, ".", profile_type,
        "&from=", from_unix, "&until=", until_unix)

    gzdata = read(outpath)
    headers = [
        "Content-Type" => "application/octet-stream",
        "Content-Encoding" => "gzip",
        "X-Profile-Lang" => "julia",
        "X-Profile-Format" => "pprof"
    ]
    if haskey(ENV, "PYROSCOPE_AUTH")
        push!(headers, "Authorization" => "Bearer $(ENV["PYROSCOPE_AUTH"])")
    end

    try
        resp = HTTP.post(url, headers, gzdata)
        println("[Pyroscope] Uploaded $(profile_type) profile (status=$(resp.status))")
    catch e
        @warn "[Pyroscope] Upload failed for $(profile_type): $e"
    end
end

# ------------------------------------------------------------------
# Continuous profiling controller with graceful shutdown via Ctrl+C
# ------------------------------------------------------------------

const profiler_tasks = Ref{Vector{Task}}(Vector{Task}())
const shutdown_flag = Ref(false)

function run_profiler_task(kind::Symbol, config::ProfilerConfig)
    @spawn begin
        while !shutdown_flag[] && ((kind == :cpu && config.enabled_cpu) || (kind == :allocs && config.enabled_alloc))
            try
                file, t_start, t_end = collect_pprof_snapshot(kind; duration=config.duration,
                    outpath="$(kind)_profile.pb.gz")
                upload_pprof(file; t_start=t_start, t_end=t_end, profile_type=string(kind))
            catch e
                @warn "$(kind) profiling iteration failed: $e"
            end
            # sleep(config.interval)
        end
        println("$(kind) profiler task stopped.")
    end
end

function start_profiling(; interval::Float64=60.0, duration::Float64=5.0, enable_cpu::Bool=true, enable_alloc::Bool=true)
    config = ProfilerConfig(interval, duration, enable_cpu, enable_alloc)
    println("Starting continuous profiler (Julia 1.11-compatible).")

    Profile.init(; delay=0.005)

    push!(profiler_tasks[], run_profiler_task(:cpu, config))
    push!(profiler_tasks[], run_profiler_task(:allocs, config))
end

# ------------------------------------------------------------------
# Example workload
# ------------------------------------------------------------------


function long_loop()
    for i in 1:1000
        x = rand(1000)
        s = sum(sin, x)
    end
end

function medium_loop()
    for i in 1:500
        x = rand(1000)
        s = sum(cos, x)
    end
end

function short_loop()
    for i in 1:100
        x = rand(1000)
        s = sum(tanh, x)
    end
end

function example()
    println("Starting example workload with next-gen profiling...")

    function busy_loop()
        while true
            long_loop()
            medium_loop()
            short_loop()

            x = rand(1000)
            s = sum(sin, x)
            sleep(0.01)
        end
    end

    @spawn busy_loop()
    interval = parse(Float64, get(ENV, "PYROSCOPE_INTERVAL", "30"))
    duration = parse(Float64, get(ENV, "PYROSCOPE_DURATION", "5"))
    start_profiling(interval=interval, duration=duration, enable_cpu=true, enable_alloc=true)

    # Keep main thread alive and handle Ctrl+C for graceful shutdown
    try
        while true
            sleep(1.0)
        end
    catch e
        if isa(e, InterruptException)
            println("Ctrl+C detected: shutting down profiler...")
            shutdown_flag[] = true
            # wait a bit for profiler tasks to finish
            sleep(max(interval, duration) + 1)
            println("Profiler shut down gracefully.")
        else
            rethrow(e)
        end
    end
end

end

if abspath(PROGRAM_FILE) == @__FILE__
    using .PyroscopePProf
    PyroscopePProf.example()
end
