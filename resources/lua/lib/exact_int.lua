-- Exact-integer library for Reqtide Redis scripts.
--
-- Redis runs Lua 5.1: every number is an IEEE-754 binary64 value, so integers are exact only through
-- LUA_EXACT_MAX = 2^53 - 1. This file is text that the script loader places in front of a script body; it defines the
-- local table `exact_int` and nothing else. It has no dependency and performs no Redis call.
--
-- Rules the code keeps (checked by the static scan test):
--   * no number is ever turned into text with tostring, concatenation or string.format outside format_int;
--   * integers enter as decimal strings (parse_int) or as already validated numbers (from_num);
--   * a product, a sum or a quotient near LUA_EXACT_MAX is never computed natively unless it is provably exact (below)
--     and is never "checked after the fact".
--
-- Limbs: non-negative integers in base 8192 (13 bits), least significant first, normalized (no most-significant zero;
-- zero is the empty table). A limb operation stays below 2^30, far inside the exact range.
--
-- Soundness of the native fast paths (all values are doubles holding integers in [0, LUA_EXACT_MAX]):
--   * Product: if fl(a*b) <= LUA_EXACT_MAX then a*b <= LUA_EXACT_MAX and the product is exact. Proof: floating-point
--     rounding is monotone and 2^53 is representable, so a*b >= 2^53 implies fl(a*b) >= 2^53 > LUA_EXACT_MAX.
--   * Sum: with p = a*b exact and r <= LUA_EXACT_MAX - p, p + r is exact.
--   * Quotient: only when n + d <= LUA_EXACT_MAX. Then q = floor(n / d) from the rounded quotient is off by at most one
--     (the rounding error of n / d is below one because n < 2^53), q * d <= n + d is exact, and the remainder
--     n - q * d is exact; one corrective step restores 0 <= remainder < d, and a final assertion enforces it.
--   Everything else takes the limb path, whose results are identical.

local exact_int = {}

do
    local EXACT_MAX = 9007199254740991
    local EXACT_MAX_TEXT = '9007199254740991'
    local BITS = 13
    local BASE = 8192

    exact_int.EXACT_MAX = EXACT_MAX

    local function domain_error(message)
        error(message, 0)
    end

    local function is_exact_integer(n)
        return type(n) == 'number' and n >= 0 and n <= EXACT_MAX and math.floor(n) == n
    end

    exact_int.is_exact_integer = is_exact_integer

    -- BEGIN parse_int (the only place text becomes a number)
    -- Strict decimal parser: digits only, no sign, no leading zero (except "0" itself), at most LUA_EXACT_MAX.
    -- The 16-digit case is compared as text so that no rounded conversion is ever trusted.
    function exact_int.parse_int(text)
        if type(text) ~= 'string' then
            domain_error('exact_int: not a string')
        end
        local length = string.len(text)
        if length == 0 or length > 16 or not string.find(text, '^[0-9]+$') then
            domain_error('exact_int: not a decimal integer')
        end
        if length > 1 and string.sub(text, 1, 1) == '0' then
            domain_error('exact_int: leading zero')
        end
        if length == 16 and text > EXACT_MAX_TEXT then
            domain_error('exact_int: above LUA_EXACT_MAX')
        end
        return tonumber(text)
    end
    -- END parse_int

    -- BEGIN format_int (the only integer serializer; "%.0f" is exact for doubles through 2^53)
    function exact_int.format_int(n)
        if not is_exact_integer(n) then
            domain_error('exact_int: cannot format')
        end
        return string.format('%.0f', n)
    end
    -- END format_int

    local function trim(limbs)
        local top = #limbs
        while top > 0 and limbs[top] == 0 do
            limbs[top] = nil
            top = top - 1
        end
        return limbs
    end

    function exact_int.from_num(n)
        if not is_exact_integer(n) then
            domain_error('exact_int: operand outside the exact range')
        end
        local limbs = {}
        local count = 0
        while n > 0 do
            local rest = n % BASE
            count = count + 1
            limbs[count] = rest
            n = (n - rest) / BASE
        end
        return limbs
    end

    -- Native value of a limb vector, or nil when it exceeds LUA_EXACT_MAX.
    function exact_int.to_num(limbs)
        local count = #limbs
        if count > 5 then
            return nil
        end
        local value = 0
        for i = count, 1, -1 do
            if value > (EXACT_MAX - limbs[i]) / BASE then
                return nil
            end
            value = value * BASE + limbs[i]
        end
        if value > EXACT_MAX then
            return nil
        end
        return value
    end

    function exact_int.compare(a, b)
        local na, nb = #a, #b
        if na ~= nb then
            return na < nb and -1 or 1
        end
        for i = na, 1, -1 do
            if a[i] ~= b[i] then
                return a[i] < b[i] and -1 or 1
            end
        end
        return 0
    end

    function exact_int.add(a, b)
        local n = #a
        if #b > n then
            n = #b
        end
        local result = {}
        local carry = 0
        for i = 1, n do
            local sum = (a[i] or 0) + (b[i] or 0) + carry
            local rest = sum % BASE
            result[i] = rest
            carry = (sum - rest) / BASE
        end
        if carry > 0 then
            result[n + 1] = carry
        end
        return result
    end

    local function subtract_unchecked(a, b)
        local result = {}
        local borrow = 0
        for i = 1, #a do
            local diff = a[i] - (b[i] or 0) - borrow
            if diff < 0 then
                diff = diff + BASE
                borrow = 1
            else
                borrow = 0
            end
            result[i] = diff
        end
        return trim(result)
    end

    function exact_int.sub(a, b)
        if exact_int.compare(a, b) < 0 then
            domain_error('exact_int: subtraction below zero')
        end
        return subtract_unchecked(a, b)
    end

    -- Schoolbook convolution, then carry normalization. One cell sums at most min(#a, #b) products of two 13-bit limbs.
    function exact_int.mul(a, b)
        local na, nb = #a, #b
        if na == 0 or nb == 0 then
            return {}
        end
        local cells = {}
        for k = 1, na + nb do
            cells[k] = 0
        end
        for i = 1, na do
            local ai = a[i]
            if ai ~= 0 then
                for j = 1, nb do
                    cells[i + j - 1] = cells[i + j - 1] + ai * b[j]
                end
            end
        end
        local result = {}
        local carry = 0
        for k = 1, na + nb do
            local total = cells[k] + carry
            local rest = total % BASE
            result[k] = rest
            carry = (total - rest) / BASE
        end
        return trim(result)
    end

    -- Binary long division over limbs: for each source bit from the most significant, shift the remainder left,
    -- append the bit and subtract the divisor once when the remainder is not smaller.
    function exact_int.divmod(numerator, divisor)
        if #divisor == 0 then
            domain_error('exact_int: division by zero')
        end
        if exact_int.compare(numerator, divisor) < 0 then
            return {}, numerator
        end
        local count = #numerator
        local quotient = {}
        for i = 1, count do
            quotient[i] = 0
        end
        local remainder = {}
        for index = count, 1, -1 do
            local limb = numerator[index]
            for bit = BITS - 1, 0, -1 do
                local power = 2 ^ bit
                local incoming = math.floor(limb / power) % 2
                local carry = incoming
                for k = 1, #remainder do
                    local shifted = remainder[k] * 2 + carry
                    if shifted >= BASE then
                        remainder[k] = shifted - BASE
                        carry = 1
                    else
                        remainder[k] = shifted
                        carry = 0
                    end
                end
                if carry > 0 then
                    remainder[#remainder + 1] = carry
                end
                if exact_int.compare(remainder, divisor) >= 0 then
                    remainder = subtract_unchecked(remainder, divisor)
                    quotient[index] = quotient[index] + power
                end
            end
        end
        return trim(quotient), remainder
    end

    -- True when a * b is provably exact in a double (see the header).
    local function product_fits(a, b)
        return a * b <= EXACT_MAX
    end

    -- Exact comparison of a * b with c * d: -1, 0 or 1.
    function exact_int.compare_products(a, b, c, d)
        if not (is_exact_integer(a) and is_exact_integer(b) and is_exact_integer(c) and is_exact_integer(d)) then
            domain_error('exact_int: operand outside the exact range')
        end
        if product_fits(a, b) and product_fits(c, d) then
            local left, right = a * b, c * d
            if left < right then
                return -1
            elseif left > right then
                return 1
            end
            return 0
        end
        return exact_int.compare(
            exact_int.mul(exact_int.from_num(a), exact_int.from_num(b)),
            exact_int.mul(exact_int.from_num(c), exact_int.from_num(d))
        )
    end

    -- Exact comparison of a * b + c * d with e * f: -1, 0 or 1 (the sliding-counter admission inequality).
    function exact_int.compare_weighted_sum(a, b, c, d, e, f)
        if not (is_exact_integer(a) and is_exact_integer(b) and is_exact_integer(c) and is_exact_integer(d)
            and is_exact_integer(e) and is_exact_integer(f)) then
            domain_error('exact_int: operand outside the exact range')
        end
        if product_fits(a, b) and product_fits(c, d) and product_fits(e, f) then
            local first, second = a * b, c * d
            if first <= EXACT_MAX - second then
                local left, right = first + second, e * f
                if left < right then
                    return -1
                elseif left > right then
                    return 1
                end
                return 0
            end
        end
        return exact_int.compare(
            exact_int.add(
                exact_int.mul(exact_int.from_num(a), exact_int.from_num(b)),
                exact_int.mul(exact_int.from_num(c), exact_int.from_num(d))
            ),
            exact_int.mul(exact_int.from_num(e), exact_int.from_num(f))
        )
    end

    -- Returns quotient, capped. quotient = min(ceil(max(a * b - c, 0) / d), cap) (the conservative wait:
    -- never rounded down). The divisor must be positive.
    function exact_int.mul_sub_ceil_div_capped(a, b, c, d, cap)
        if not (is_exact_integer(a) and is_exact_integer(b) and is_exact_integer(c) and is_exact_integer(cap)
            and is_exact_integer(d) and d > 0) then
            domain_error('exact_int: operand outside the exact range')
        end

        local p = a * b
        if p <= EXACT_MAX then
            local n = 0
            if p > c then
                n = p - c
            end
            if n <= EXACT_MAX - d then
                local q = math.floor(n / d)
                local rem = n - q * d
                if rem < 0 then
                    q = q - 1
                    rem = rem + d
                elseif rem >= d then
                    q = q + 1
                    rem = rem - d
                end
                if rem < 0 or rem >= d then
                    domain_error('exact_int: native division invariant violated')
                end
                if rem > 0 then
                    q = q + 1
                end
                if q >= cap then
                    return cap, true
                end
                return q, false
            end
        end

        local product = exact_int.mul(exact_int.from_num(a), exact_int.from_num(b))
        local subtrahend = exact_int.from_num(c)
        local numerator = {}
        if exact_int.compare(product, subtrahend) > 0 then
            numerator = exact_int.sub(product, subtrahend)
        end
        local quotient, remainder = exact_int.divmod(numerator, exact_int.from_num(d))
        if #remainder > 0 then
            quotient = exact_int.add(quotient, { 1 })
        end
        if exact_int.compare(quotient, exact_int.from_num(cap)) >= 0 then
            return cap, true
        end
        return exact_int.to_num(quotient), false
    end

    -- Returns quotient, remainder, capped. quotient = min(floor((a * b + r) / d), cap). When the quotient reaches the
    -- cap the result is (cap, 0, true) and the remainder is discarded. The divisor must be positive.
    function exact_int.mul_add_div_floor_capped(a, b, r, d, cap)
        if not (is_exact_integer(a) and is_exact_integer(b) and is_exact_integer(r) and is_exact_integer(cap)
            and is_exact_integer(d) and d > 0) then
            domain_error('exact_int: operand outside the exact range')
        end

        local p = a * b
        if p <= EXACT_MAX and r <= EXACT_MAX - p then
            local n = p + r
            if n <= EXACT_MAX - d then
                local q = math.floor(n / d)
                local rem = n - q * d
                if rem < 0 then
                    q = q - 1
                    rem = rem + d
                elseif rem >= d then
                    q = q + 1
                    rem = rem - d
                end
                if rem < 0 or rem >= d then
                    domain_error('exact_int: native division invariant violated')
                end
                if q >= cap then
                    return cap, 0, true
                end
                return q, rem, false
            end
        end

        local quotient, remainder = exact_int.divmod(
            exact_int.add(exact_int.mul(exact_int.from_num(a), exact_int.from_num(b)), exact_int.from_num(r)),
            exact_int.from_num(d)
        )
        if exact_int.compare(quotient, exact_int.from_num(cap)) >= 0 then
            return cap, 0, true
        end
        return exact_int.to_num(quotient), exact_int.to_num(remainder), false
    end
end
