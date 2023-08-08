from math import e, log

try:
	from scipy.stats import norm
except ImportError:
	print('Mibian requires scipy to work properly')

# WARNING: All numbers should be floats -> x = 1.0

def impliedVolatility(className, args, callPrice=None, putPrice=None, high=500.0, low=0.0):
	'''Returns the estimated implied volatility'''
	if callPrice:
		target = callPrice
		restimate = eval(className)(args, volatility=high, performance=True).callPrice  
		if restimate < target:
			return high
		if args[0]>args[1] + callPrice:
			return 0.001            
	if putPrice:
		target = putPrice
		restimate = eval(className)(args, volatility=high, performance=True).putPrice
		if restimate < target:
			return high
		if args[1]>args[0] + putPrice:
			return 0.001            
	decimals = len(str(target).split('.')[1])		# Count decimals
	for i in range(10000):	# To avoid infinite loops
		mid = (high + low) / 2
		if mid < 0.00001:
			mid = 0.00001
		if callPrice:
			estimate = eval(className)(args, volatility=mid, performance=True).callPrice
		if putPrice:
			estimate = eval(className)(args, volatility=mid, performance=True).putPrice
		if round(estimate, decimals) == target: 
			break
		elif estimate > target: 
			high = mid
		elif estimate < target: 
			low = mid
	return mid

class GK:
	'''Garman-Kohlhagen
	Used for pricing European options on currencies
	
	GK([underlyingPrice, strikePrice, domesticRate, foreignRate, \
			daysToExpiration], volatility=x, callPrice=y, putPrice=z)

	eg: 
		c = mibian.GK([1.4565, 1.45, 1, 2, 30], volatility=20)
		c.callPrice				# Returns the call price
		c.putPrice				# Returns the put price
		c.callDelta				# Returns the call delta
		c.putDelta				# Returns the put delta
		c.callDelta2			# Returns the call dual delta
		c.putDelta2				# Returns the put dual delta
		c.callTheta				# Returns the call theta
		c.putTheta				# Returns the put theta
		c.callRhoD				# Returns the call domestic rho
		c.putRhoD				# Returns the put domestic rho
		c.callRhoF				# Returns the call foreign rho
		c.putRhoF				# Returns the call foreign rho
		c.vega					# Returns the option vega
		c.gamma					# Returns the option gamma

		c = mibian.GK([1.4565, 1.45, 1, 2, 30], callPrice=0.0359)
		c.impliedVolatility		# Returns the implied volatility from the call price
		
		c = mibian.GK([1.4565, 1.45, 1, 2, 30], putPrice=0.03)
		c.impliedVolatility		# Returns the implied volatility from the put price
		
		c = mibian.GK([1.4565, 1.45, 1, 2, 30], callPrice=0.0359, putPrice=0.03)
		c.putCallParity			# Returns the put-call parity
	'''

	def __init__(self, args, volatility=None, callPrice=None, putPrice=None, \
			performance=None):
		self.underlyingPrice = float(args[0])
		self.strikePrice = float(args[1])
		self.domesticRate = float(args[2]) / 100
		self.foreignRate = float(args[3]) / 100
		self.daysToExpiration = float(args[4]) / 365

		for i in ['callPrice', 'putPrice', 'callDelta', 'putDelta', \
				'callDelta2', 'putDelta2', 'callTheta', 'putTheta', \
				'callRhoD', 'putRhoD', 'callRhoF', 'callRhoF', 'vega', \
				'gamma', 'impliedVolatility', 'putCallParity']:
			self.__dict__[i] = None
		
		if volatility:
			self.volatility = float(volatility) / 100

			self._a_ = self.volatility * self.daysToExpiration**0.5
			self._d1_ = (log(self.underlyingPrice / self.strikePrice) + \
				(self.domesticRate - self.foreignRate + \
				(self.volatility**2)/2) * self.daysToExpiration) / self._a_
			self._d2_ = self._d1_ - self._a_
			# Reduces performance overhead when computing implied volatility
			if performance:		
				[self.callPrice, self.putPrice] = self._price()
			else:
				[self.callPrice, self.putPrice] = self._price()
				[self.callDelta, self.putDelta] = self._delta()
				[self.callDelta2, self.putDelta2] = self._delta2()
				[self.callTheta, self.putTheta] = self._theta()
				[self.callRhoD, self.putRhoD] = self._rhod()
				[self.callRhoF, self.putRhoF] = self._rhof()
				self.vega = self._vega()
				self.gamma = self._gamma()
				self.exerciceProbability = norm.cdf(self._d2_)
		if callPrice:
			self.callPrice = round(float(callPrice), 6)
			self.impliedVolatility = impliedVolatility(\
					self.__class__.__name__, args, callPrice=self.callPrice)
		if putPrice and not callPrice:
			self.putPrice = round(float(putPrice), 6)
			self.impliedVolatility = impliedVolatility(\
					self.__class__.__name__, args, putPrice=self.putPrice)
		if callPrice and putPrice:
			self.callPrice = float(callPrice)
			self.putPrice = float(putPrice)
			self.putCallParity = self._parity()

	def _price(self):
		'''Returns the option price: [Call price, Put price]'''
		if self.volatility == 0 or self.daysToExpiration == 0:
			call = max(0.0, self.underlyingPrice - self.strikePrice)
			put = max(0.0, self.strikePrice - self.underlyingPrice)
		if self.strikePrice == 0:
			raise ZeroDivisionError('The strike price cannot be zero')
		else:
			call = e**(-self.foreignRate * self.daysToExpiration) * \
					self.underlyingPrice * norm.cdf(self._d1_) - \
					e**(-self.domesticRate * self.daysToExpiration) * \
					self.strikePrice * norm.cdf(self._d2_)
			put = e**(-self.domesticRate * self.daysToExpiration) * \
					self.strikePrice * norm.cdf(-self._d2_) - \
					e**(-self.foreignRate * self.daysToExpiration) * \
					self.underlyingPrice * norm.cdf(-self._d1_)
		return [call, put]

	def _delta(self):
		'''Returns the option delta: [Call delta, Put delta]'''
		if self.volatility == 0 or self.daysToExpiration == 0:
			call = 1.0 if self.underlyingPrice > self.strikePrice else 0.0
			put = -1.0 if self.underlyingPrice < self.strikePrice else 0.0
		if self.strikePrice == 0:
			raise ZeroDivisionError('The strike price cannot be zero')
		else:
			_b_ = e**-(self.foreignRate * self.daysToExpiration)
			call = norm.cdf(self._d1_) * _b_
			put = -norm.cdf(-self._d1_) * _b_
		return [call, put]

	def _delta2(self):
		'''Returns the dual delta: [Call dual delta, Put dual delta]'''
		if self.volatility == 0 or self.daysToExpiration == 0:
			call = -1.0 if self.underlyingPrice > self.strikePrice else 0.0
			put = 1.0 if self.underlyingPrice < self.strikePrice else 0.0
		if self.strikePrice == 0:
			raise ZeroDivisionError('The strike price cannot be zero')
		else:
			_b_ = e**-(self.domesticRate * self.daysToExpiration)
			call = -norm.cdf(self._d2_) * _b_
			put = norm.cdf(-self._d2_) * _b_
		return [call, put]

	def _vega(self):
		'''Returns the option vega'''
		if self.volatility == 0 or self.daysToExpiration == 0:
			return 0.0
		if self.strikePrice == 0:
			raise ZeroDivisionError('The strike price cannot be zero')
		else:
			return self.underlyingPrice * e**-(self.foreignRate * \
					self.daysToExpiration) * norm.pdf(self._d1_) * \
					self.daysToExpiration**0.5

	def _theta(self):
		'''Returns the option theta: [Call theta, Put theta]'''
		_b_ = e**-(self.foreignRate * self.daysToExpiration)
		call = -self.underlyingPrice * _b_ * norm.pdf(self._d1_) * \
				self.volatility / (2 * self.daysToExpiration**0.5) + \
				self.foreignRate * self.underlyingPrice * _b_ * \
				norm.cdf(self._d1_) - self.domesticRate * self.strikePrice * \
				_b_ * norm.cdf(self._d2_)
		put = -self.underlyingPrice * _b_ * norm.pdf(self._d1_) * \
				self.volatility / (2 * self.daysToExpiration**0.5) - \
				self.foreignRate * self.underlyingPrice * _b_ * \
				norm.cdf(-self._d1_) + self.domesticRate * self.strikePrice * \
				_b_ * norm.cdf(-self._d2_)
		return [call / 365, put / 365]

	def _rhod(self):
		'''Returns the option domestic rho: [Call rho, Put rho]'''
		call = self.strikePrice * self.daysToExpiration * \
				e**(-self.domesticRate * self.daysToExpiration) * \
				norm.cdf(self._d2_) / 100
		put = -self.strikePrice * self.daysToExpiration * \
				e**(-self.domesticRate * self.daysToExpiration) * \
				norm.cdf(-self._d2_) / 100
		return [call, put]

	def _rhof(self):
		'''Returns the option foreign rho: [Call rho, Put rho]'''
		call = -self.underlyingPrice * self.daysToExpiration * \
				e**(-self.foreignRate * self.daysToExpiration) * \
				norm.cdf(self._d1_) / 100
		put = self.underlyingPrice * self.daysToExpiration * \
				e**(-self.foreignRate * self.daysToExpiration) * \
				norm.cdf(-self._d1_) / 100
		return [call, put]

	def _gamma(self):
		'''Returns the option gamma'''
		return (norm.pdf(self._d1_) * e**-(self.foreignRate * \
				self.daysToExpiration)) / (self.underlyingPrice * self._a_)

	def _parity(self):
		'''Returns the put-call parity'''
		return self.callPrice - self.putPrice - (self.underlyingPrice / \
				((1 + self.foreignRate)**self.daysToExpiration)) + \
				(self.strikePrice / \
				((1 + self.domesticRate)**self.daysToExpiration))

class BS:
	'''Black-Scholes
	Used for pricing European options on stocks without dividends

	BS([underlyingPrice, strikePrice, interestRate, daysToExpiration], \
			volatility=x, callPrice=y, putPrice=z)

	eg: 
		c = mibian.BS([1.4565, 1.45, 1, 30], volatility=20)
		c.callPrice				# Returns the call price
		c.putPrice				# Returns the put price
		c.callDelta				# Returns the call delta
		c.putDelta				# Returns the put delta
		c.callDelta2			# Returns the call dual delta
		c.putDelta2				# Returns the put dual delta
		c.callTheta				# Returns the call theta
		c.putTheta				# Returns the put theta
		c.callRho				# Returns the call rho
		c.putRho				# Returns the put rho
		c.vega					# Returns the option vega
		c.gamma					# Returns the option gamma

		c = mibian.BS([1.4565, 1.45, 1, 30], callPrice=0.0359)
		c.impliedVolatility		# Returns the implied volatility from the call price
		
		c = mibian.BS([1.4565, 1.45, 1, 30], putPrice=0.0306)
		c.impliedVolatility		# Returns the implied volatility from the put price
		
		c = mibian.BS([1.4565, 1.45, 1, 30], callPrice=0.0359, putPrice=0.0306)
		c.putCallParity			# Returns the put-call parity
		'''

	def __init__(self, args, volatility=None, callPrice=None, putPrice=None, \
			performance=None):
		self.underlyingPrice = float(args[0])
		self.strikePrice = float(args[1])
		self.interestRate = float(args[2]) / 100
		self.daysToExpiration = float(args[3]) / 365

		for i in ['callPrice', 'putPrice', 'callDelta', 'putDelta', \
				'callDelta2', 'putDelta2', 'callTheta', 'putTheta', \
				'callRho', 'putRho', 'vega', 'gamma', 'impliedVolatility', \
				'putCallParity']:
			self.__dict__[i] = None
		
		if volatility:
			self.volatility = float(volatility) / 100

			self._a_ = self.volatility * self.daysToExpiration ** 0.5
			try:
				self._d1_ = (log(self.underlyingPrice / self.strikePrice) + \
						(self.interestRate + (self.volatility**2) / 2) * \
						self.daysToExpiration) / self._a_
			except ZeroDivisionError:
				# TODO: This happens when daysToExpiration is zero, how should we deal with this?
				self._d1_ = 0
    
			self._d2_ = self._d1_ - self._a_
			if performance:
				[self.callPrice, self.putPrice] = self._price()
			else:
				[self.callPrice, self.putPrice] = self._price()
				[self.callDelta, self.putDelta] = self._delta()
				[self.callDelta2, self.putDelta2] = self._delta2()
				[self.callTheta, self.putTheta] = self._theta()
				[self.callRho, self.putRho] = self._rho()
				self.vega = self._vega()
				self.gamma = self._gamma()
				self.exerciceProbability = norm.cdf(self._d2_)
		if callPrice:
			self.callPrice = round(float(callPrice), 6)
			self.impliedVolatility = impliedVolatility(\
					self.__class__.__name__, args, callPrice=self.callPrice)
		if putPrice and not callPrice:
			self.putPrice = round(float(putPrice), 6)
			self.impliedVolatility = impliedVolatility(\
					self.__class__.__name__, args, putPrice=self.putPrice)
		if callPrice and putPrice:
			self.callPrice = float(callPrice)
			self.putPrice = float(putPrice)
			self.putCallParity = self._parity()

	def _price(self):
		'''Returns the option price: [Call price, Put price]'''
		if self.volatility == 0 or self.daysToExpiration == 0:
			call = max(0.0, self.underlyingPrice - self.strikePrice)
			put = max(0.0, self.strikePrice - self.underlyingPrice)
		if self.strikePrice == 0:
			raise ZeroDivisionError('The strike price cannot be zero')
		else:
			call = self.underlyingPrice * norm.cdf(self._d1_) - \
					self.strikePrice * e**(-self.interestRate * \
					self.daysToExpiration) * norm.cdf(self._d2_)
			put = self.strikePrice * e**(-self.interestRate * \
					self.daysToExpiration) * norm.cdf(-self._d2_) - \
					self.underlyingPrice * norm.cdf(-self._d1_)
		return [call, put]

	def _delta(self):
		'''Returns the option delta: [Call delta, Put delta]'''
		if self.volatility == 0 or self.daysToExpiration == 0:
			call = 1.0 if self.underlyingPrice > self.strikePrice else 0.0
			put = -1.0 if self.underlyingPrice < self.strikePrice else 0.0
		if self.strikePrice == 0:
			raise ZeroDivisionError('The strike price cannot be zero')
		else:
			call = norm.cdf(self._d1_)
			put = -norm.cdf(-self._d1_)
		return [call, put]

	def _delta2(self):
		'''Returns the dual delta: [Call dual delta, Put dual delta]'''
		if self.volatility == 0 or self.daysToExpiration == 0:
			call = -1.0 if self.underlyingPrice > self.strikePrice else 0.0
			put = 1.0 if self.underlyingPrice < self.strikePrice else 0.0
		if self.strikePrice == 0:
			raise ZeroDivisionError('The strike price cannot be zero')
		else:
			_b_ = e**-(self.interestRate * self.daysToExpiration)
			call = -norm.cdf(self._d2_) * _b_
			put = norm.cdf(-self._d2_) * _b_
		return [call, put]

	def _vega(self):
		'''Returns the option vega'''
		if self.volatility == 0 or self.daysToExpiration == 0:
			return 0.0
		if self.strikePrice == 0:
			raise ZeroDivisionError('The strike price cannot be zero')
		else:
			return self.underlyingPrice * norm.pdf(self._d1_) * \
					self.daysToExpiration**0.5 / 100

	def _theta(self):
		'''Returns the option theta: [Call theta, Put theta]'''
		_b_ = e**-(self.interestRate * self.daysToExpiration)
		call = -self.underlyingPrice * norm.pdf(self._d1_) * self.volatility / \
				(2 * self.daysToExpiration**0.5) - self.interestRate * \
				self.strikePrice * _b_ * norm.cdf(self._d2_)
		put = -self.underlyingPrice * norm.pdf(self._d1_) * self.volatility / \
				(2 * self.daysToExpiration**0.5) + self.interestRate * \
				self.strikePrice * _b_ * norm.cdf(-self._d2_)
		return [call / 365, put / 365]

	def _rho(self):
		'''Returns the option rho: [Call rho, Put rho]'''
		_b_ = e**-(self.interestRate * self.daysToExpiration)
		call = self.strikePrice * self.daysToExpiration * _b_ * \
				norm.cdf(self._d2_) / 100
		put = -self.strikePrice * self.daysToExpiration * _b_ * \
				norm.cdf(-self._d2_) / 100
		return [call, put]

	def _gamma(self):
		'''Returns the option gamma'''
		return norm.pdf(self._d1_) / (self.underlyingPrice * self._a_)

	def _parity(self):
		'''Put-Call Parity'''
		return self.callPrice - self.putPrice - self.underlyingPrice + \
				(self.strikePrice / \
				((1 + self.interestRate)**self.daysToExpiration))

class Me:
	'''Merton
	Used for pricing European options on stocks with dividends

	Me([underlyingPrice, strikePrice, interestRate, annualDividends, \
			daysToExpiration], volatility=x, callPrice=y, putPrice=z)

	eg: 
		c = mibian.Me([52, 50, 1, 1, 30], volatility=20)
		c.callPrice				# Returns the call price
		c.putPrice				# Returns the put price
		c.callDelta				# Returns the call delta
		c.putDelta				# Returns the put delta
		c.callDelta2			# Returns the call dual delta
		c.putDelta2				# Returns the put dual delta
		c.callTheta				# Returns the call theta
		c.putTheta				# Returns the put theta
		c.callRho				# Returns the call rho
		c.putRho				# Returns the put rho
		c.vega					# Returns the option vega
		c.gamma					# Returns the option gamma

		c = mibian.Me([52, 50, 1, 1, 30], callPrice=0.0359)
		c.impliedVolatility		# Returns the implied volatility from the call price
		
		c = mibian.Me([52, 50, 1, 1, 30], putPrice=0.0306)
		c.impliedVolatility		# Returns the implied volatility from the put price
		
		c = mibian.Me([52, 50, 1, 1, 30], callPrice=0.0359, putPrice=0.0306)
		c.putCallParity			# Returns the put-call parity
	'''

	def __init__(self, args, volatility=None, callPrice=None, putPrice=None, \
			performance=None):
		self.underlyingPrice = float(args[0])
		self.strikePrice = float(args[1])
		self.interestRate = float(args[2]) / 100
		self.dividend = float(args[3])
		self.dividendYield = self.dividend / self.underlyingPrice
		self.daysToExpiration = float(args[4]) / 365

		for i in ['callPrice', 'putPrice', 'callDelta', 'putDelta', \
				'callDelta2', 'putDelta2', 'callTheta', 'putTheta', \
				'callRho', 'putRho', 'vega', 'gamma', 'impliedVolatility', \
				'putCallParity']:
			self.__dict__[i] = None
		
		if volatility:
			self.volatility = float(volatility) / 100

			self._a_ = self.volatility * self.daysToExpiration**0.5
			self._d1_ = (log(self.underlyingPrice / self.strikePrice) + \
					(self.interestRate - self.dividendYield + \
					(self.volatility**2) / 2) * self.daysToExpiration) / \
					self._a_
			self._d2_ = self._d1_ - self._a_
			if performance:
				[self.callPrice, self.putPrice] = self._price()
			else:
				[self.callPrice, self.putPrice] = self._price()
				[self.callDelta, self.putDelta] = self._delta()
				[self.callDelta2, self.putDelta2] = self._delta2()
				[self.callTheta, self.putTheta] = self._theta()
				[self.callRho, self.putRho] = self._rho()
				self.vega = self._vega()
				self.gamma = self._gamma()
				self.exerciceProbability = norm.cdf(self._d2_)
		if callPrice:
			self.callPrice = round(float(callPrice), 6)
			self.impliedVolatility = impliedVolatility(\
					self.__class__.__name__, args, self.callPrice)
		if putPrice and not callPrice:
			self.putPrice = round(float(putPrice), 6)
			self.impliedVolatility = impliedVolatility(\
					self.__class__.__name__, args, putPrice=self.putPrice)
		if callPrice and putPrice:
			self.callPrice = float(callPrice)
			self.putPrice = float(putPrice)
			self.putCallParity = self._parity()

	def _price(self):
		'''Returns the option price: [Call price, Put price]'''
		if self.volatility == 0 or self.daysToExpiration == 0:
			call = max(0.0, self.underlyingPrice - self.strikePrice)
			put = max(0.0, self.strikePrice - self.underlyingPrice)
		if self.strikePrice == 0:
			raise ZeroDivisionError('The strike price cannot be zero')
		else:
			call = self.underlyingPrice * e**(-self.dividendYield * \
					self.daysToExpiration) * norm.cdf(self._d1_) - \
					self.strikePrice * e**(-self.interestRate * \
					self.daysToExpiration) * norm.cdf(self._d2_)
			put = self.strikePrice * e**(-self.interestRate * \
					self.daysToExpiration) * norm.cdf(-self._d2_) - \
					self.underlyingPrice * e**(-self.dividendYield * \
					self.daysToExpiration) * norm.cdf(-self._d1_)
		return [call, put]

	def _delta(self):
		'''Returns the option delta: [Call delta, Put delta]'''
		if self.volatility == 0 or self.daysToExpiration == 0:
			call = 1.0 if self.underlyingPrice > self.strikePrice else 0.0
			put = -1.0 if self.underlyingPrice < self.strikePrice else 0.0
		if self.strikePrice == 0:
			raise ZeroDivisionError('The strike price cannot be zero')
		else:
			_b_ = e**(-self.dividendYield * self.daysToExpiration)
			call = _b_ * norm.cdf(self._d1_)
			put = _b_ *	(norm.cdf(self._d1_) - 1)
		return [call, put]

	# Verify
	def _delta2(self):
		'''Returns the dual delta: [Call dual delta, Put dual delta]'''
		if self.volatility == 0 or self.daysToExpiration == 0:
			call = -1.0 if self.underlyingPrice > self.strikePrice else 0.0
			put = 1.0 if self.underlyingPrice < self.strikePrice else 0.0
		if self.strikePrice == 0:
			raise ZeroDivisionError('The strike price cannot be zero')
		else:
			_b_ = e**-(self.interestRate * self.daysToExpiration)
			call = -norm.cdf(self._d2_) * _b_
			put = norm.cdf(-self._d2_) * _b_
		return [call, put]

	def _vega(self):
		'''Returns the option vega'''
		if self.volatility == 0 or self.daysToExpiration == 0:
			return 0.0
		if self.strikePrice == 0:
			raise ZeroDivisionError('The strike price cannot be zero')
		else:
			return self.underlyingPrice * e**(-self.dividendYield * \
					self.daysToExpiration) * norm.pdf(self._d1_) * \
					self.daysToExpiration**0.5 / 100

	def _theta(self):
		'''Returns the option theta: [Call theta, Put theta]'''
		_b_ = e**-(self.interestRate * self.daysToExpiration)
		_d_ = e**(-self.dividendYield * self.daysToExpiration)
		call = -self.underlyingPrice * _d_ * norm.pdf(self._d1_) * \
				self.volatility / (2 * self.daysToExpiration**0.5) + \
				self.dividendYield * self.underlyingPrice * _d_ * \
				norm.cdf(self._d1_) - self.interestRate * \
				self.strikePrice * _b_ * norm.cdf(self._d2_)
		put = -self.underlyingPrice * _d_ * norm.pdf(self._d1_) * \
				self.volatility / (2 * self.daysToExpiration**0.5) - \
				self.dividendYield * self.underlyingPrice * _d_ * \
				norm.cdf(-self._d1_) + self.interestRate * \
				self.strikePrice * _b_ * norm.cdf(-self._d2_)
		return [call / 365, put / 365]

	def _rho(self):
		'''Returns the option rho: [Call rho, Put rho]'''
		_b_ = e**-(self.interestRate * self.daysToExpiration)
		call = self.strikePrice * self.daysToExpiration * _b_ * \
				norm.cdf(self._d2_) / 100
		put = -self.strikePrice * self.daysToExpiration * _b_ * \
				norm.cdf(-self._d2_) / 100
		return [call, put]

	def _gamma(self):
		'''Returns the option gamma'''
		return e**(-self.dividendYield * self.daysToExpiration) * \
				norm.pdf(self._d1_) / (self.underlyingPrice * self._a_)

	# Verify
	def _parity(self):
		'''Put-Call Parity'''
		return self.callPrice - self.putPrice - self.underlyingPrice + \
				(self.strikePrice / \
				((1 + self.interestRate)**self.daysToExpiration))

