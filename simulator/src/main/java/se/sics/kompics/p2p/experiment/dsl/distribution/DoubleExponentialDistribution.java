/**
 * This file is part of the Kompics P2P Framework.
 * 
 * Copyright (C) 2009 Swedish Institute of Computer Science (SICS)
 * Copyright (C) 2009 Royal Institute of Technology (KTH)
 *
 * Kompics is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.sics.kompics.p2p.experiment.dsl.distribution;

import java.util.Random;

/**
 * The <code>DoubleExponentialDistribution</code> class.
 * 
 * @author Cosmin Arad {@literal <cosmin@sics.se>}
 * @version $Id: DoubleExponentialDistribution.java 750 2009-04-02 09:55:01Z
 *          Cosmin $
 */
public class DoubleExponentialDistribution extends Distribution<Double> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8526513031094365461L;

	private final Random random;

	private final double mean;

	public DoubleExponentialDistribution(Double mean, Random random) {
		super(Type.EXPONENTIAL, Double.class);
		this.random = random;
		this.mean = mean;
	}

	@Override
	public final Double draw() {
		double u = random.nextDouble();
		return -mean * Math.log(1 - u);
	}
}
