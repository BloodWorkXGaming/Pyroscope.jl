module Pyroscope

using Profile, PProf, HTTP, Dates
using Base.Threads: @spawn, sleep

export start_profiling, example

# ------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------

@kwdef struct ProfilerConfig
    sample_threshold::Int = 50_000
    time_threshold::Period = Second(10)
    cpu_sample_interval::Float64 = 0.05
    alloc_sample_rate::Float64 = 0.001
    enable_cpu::Bool = true
    enable_alloc::Bool = true
end

# ------------------------------------------------------------------
# Upload helpers
# ------------------------------------------------------------------

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
        println("[Pyroscope.jl] Uploaded $(profile_type) profile (status=$(resp.status))")
    catch e
        @warn "[Pyroscope.jl] Upload failed for $(profile_type): $e"
    end
end

# ------------------------------------------------------------------
# Continuous profiler logic
# ------------------------------------------------------------------

const shutdown_flag = Ref(false)

function run_profiler_task(kind::Symbol, config::ProfilerConfig)
    @spawn begin
        try
            println("[Pyroscope.jl] Starting $(kind) profiler (threshold-based continuous mode)...")

            outpath = "$(kind)_profile.pb.gz"


            # Configure and start timers
            if kind == :cpu
                Profile.init(; delay=config.cpu_sample_interval)
                Profile.start_timer()
            elseif kind == :allocs
                Profile.Allocs.start(; sample_rate=config.alloc_sample_rate)
            end

            last_upload_time = now()
            while !shutdown_flag[]
                sleep(1.0)  # check once per second

                Profile.fetch()

                # Efficient count check (no copying)
                n_samples = if kind == :cpu
                    Profile.len_data()
                elseif kind == :allocs
                    # we can't get efficient information about the length of the allocations.
                    0
                else
                    0
                end

                time_since_upload = now() - last_upload_time

                # Trigger upload on count or time threshold
                if n_samples > config.sample_threshold || time_since_upload > config.time_threshold
                    println("[Pyroscope.jl] Upload triggered for $(kind): $(n_samples) samples, $(time_since_upload) elapsed")


                    # Stop, fetch, upload, clear, restart
                    if kind == :cpu
                        # we don't want to overload the system. throw away giant samples
                        if (n_samples > 250_000)
                            println("[Pyroscope.jl] throwing away to large sample size for $(kind): $(n_samples) samples")

                            Profile.stop_timer()
                            sleep(0.01)
                            Profile.clear()
                            continue
                        end

                        Profile.stop_timer()
                        t_start = last_upload_time
                        t_end = now()
                        sleep(0.01)

                        # fetch cpu data
                        data = Profile.fetch()

                        # clear profiler and start again, while profiling continues
                        Profile.clear()
                        Profile.start_timer()

                        # analyze and store data to file from where we will upload it
                        PProf.pprof(data; web=false, out=outpath)
                        upload_pprof(outpath; t_start=t_start, t_end=t_end, profile_type=string(kind))

                    elseif kind == :allocs
                        Profile.Allocs.stop()
                        sleep(0.01)
                        t_start = last_upload_time
                        t_end = now()

                        # fetch alloc data
                        data = Profile.Allocs.fetch()

                        # clear profiler and start again, while profiling continues
                        Profile.Allocs.clear()
                        Profile.Allocs.start(; sample_rate=config.alloc_sample_rate)

                        # analyze and store data to file from where we will upload it
                        PProf.Allocs.pprof(data; web=false, out=outpath)
                        upload_pprof(outpath; t_start=t_start, t_end=t_end, profile_type=string(kind))
                    else
                        error("[Pyroscope.jl] Unknown profile kind: $kind")
                    end

                    last_upload_time = now()
                end
            end
        catch e
            println("[Pyroscope.jl] $kind-profiler failed: $e")
        end


        println("[Pyroscope.jl] $(kind) profiler stopped gracefully.")
    end
end

function start_profiling(config::ProfilerConfig=ProfilerConfig())
    println("[Pyroscope.jl] Starting Pyroscope continuous profiler...")

    if config.enable_cpu
        run_profiler_task(:cpu, config)
    end
    if config.enable_alloc
        run_profiler_task(:allocs, config)
    end
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
        end
    end

    @spawn busy_loop()
    # interval = parse(Float64, get(ENV, "PYROSCOPE_INTERVAL", "30"))
    # duration = parse(Float64, get(ENV, "PYROSCOPE_DURATION", "5"))
    start_profiling()

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
    using .Pyroscope
    Pyroscope.example()
end
