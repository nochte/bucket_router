class Redis
  class Client
    def ensure_connected
      tries = 0

      begin
        if connected?
          if Process.pid != @pid
            puts "Reconnecting"
            reconnect
          end
        else
          connect
        end

        tries += 1

        yield
      rescue ConnectionError
        disconnect

        if tries < 2 && @reconnect
          retry
        else
          raise
        end
      rescue Exception
        disconnect
        raise
      end
    end
  end
end